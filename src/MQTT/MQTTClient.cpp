#include "MQTTClient.h"

#include <iostream>
#include <stdexcept>

std::mutex MQTTClient::_dll_mtx;
MQTTDll MQTTClient::_dll;

void MQTTClient::Operacao::esperar() {
  std::unique_lock<std::mutex> lock(_finalizada_mtx);
  _finalizada_cv.wait(lock, [this]{ return _finalizada; });
  _finalizada = false;
}


void MQTTClient::Operacao::notificar_um() {
  std::lock_guard<std::mutex> lock(_finalizada_mtx);
  _finalizada = true;
  _finalizada_cv.notify_one();
}



MQTTClient::MQTTClient(const std::string &broker_uri, const std::string &client_id) :
_broker_uri(broker_uri),
_client_id(client_id),
_conexao(*this) {
  {std::lock_guard<std::mutex> lock(_dll_mtx);
    if (!_dll.loaded()) _dll.load();
  }

  int ret = _dll.create(&_client, broker_uri.c_str(), client_id.c_str(), MQTTCLIENT_PERSISTENCE_NONE, NULL);
  if (ret != MQTTASYNC_SUCCESS) {
    std::cout << "MQTTClient::MQTTClient: Não foi possível criar client MQTTAsync: " << _dll.strerror(ret) << '\n';
  }
}



void MQTTClient::Conexao::falha(void *ctx, MQTTAsync_failureData *res) {
  Conexao &conexao = *static_cast<Conexao *>(ctx);

  const char *msg = "";
  if (res->message) msg = res->message;
  std::cerr << "MQTTClient::Conexao::falha: não foi possível estabelecer uma conexão com o broker em "<< conexao._client._broker_uri << ". Código: " << res->code << ", mensagem: " << msg << '\n';

  conexao._res = false;
  conexao.notificar_um();
}



void MQTTClient::Conexao::sucesso(void *ctx, MQTTAsync_successData *) {
  Conexao &conexao = *static_cast<Conexao *>(ctx);

  std::cout << "MQTTClient::Conexao::sucesso: conexão ao broker em " << conexao._client._broker_uri << " bem-sucedida.\n";

  conexao._res = true;
  conexao.notificar_um();
}



void MQTTClient::conexaoPerdida(void *ctx, char *cause) {
  MQTTClient &client = *static_cast<MQTTClient *>(ctx);

  std::cerr << "MQTTClient::conexaoPerdida: Conexão com o broker " << client._broker_uri << " perdida. Causa: " << cause << '\n';
}



bool MQTTClient::conectar() {
  // Não permite mais de uma thread de tentar se (des)conectar ao mesmo tempo.
  std::lock_guard<std::mutex> lock(_conexao_mtx);
  if (conectado()) return true;

  if (!_primeira_conexao) {
    int ret = _dll.reconnect(_client);
    if (ret != MQTTASYNC_SUCCESS) {
      std::cerr << "MQTTClient::conectar: Erro ao reconectar: " << _dll.strerror(ret) << '\n';
    }

  } else {
    // Configurar callbacks iniciais. Por hora, ignorar todas as mensagens recebidas.
    int ret = _dll.setCallbacks(_client, &_conexao, conexaoPerdida, mensagemRecebida, NULL);
    if (ret != MQTTASYNC_SUCCESS) {
      std::cerr << "MQTTClient::conectar: Erro ao adicionar callbacks: " << _dll.strerror(ret) << '\n';
      return false;
    }

    // Configurar as opções de conexão.
    MQTTAsync_connectOptions opts = MQTTAsync_connectOptions_initializer;
    opts.context = &_conexao;
    opts.onFailure = _conexao.falha;
    opts.onSuccess = _conexao.sucesso;
    opts.keepAliveInterval = _KEEPALIVE_INTERVAL; // KeepAlive TCP
    opts.cleansession = _CLEANSESSION;            // Todas as vezes que uma nova conexão for estabelecida, a sessão será resetada
    opts.connectTimeout = _CONNECT_TOUT;          // Timout da tentativa de conexão
    opts.retryInterval = _RETRY_INTERVAL;         // Intervalo de tentativas de envio de mensagens não reconhecidas.
    ret = _dll.connect(_client, &opts);
    if (ret != MQTTASYNC_SUCCESS) {
      std::cerr << "MQTTClient::conectar: Erro ao conectar: " << _dll.strerror(ret) << '\n';
      return false;
    }
  }

  // Esperar pela tentativa de conexão terminar.
  _conexao.esperar();

  // Retornar o estado da conexão e setar flag.
  if (_conexao.resultado()) {
    _primeira_conexao = false;
    return true;
  } else return false;
}



int MQTTClient::mensagemRecebida(void *op, char *topico, int topico_sz, MQTTAsync_message *msg) {
  MQTTClient &client = static_cast<Conexao *>(op)->getClient();

  AcquireSRWLockShared(&client._callback_map_slock);
  std::unordered_map<std::string, MQTTAsync_messageArrived *>::iterator iter = client._callback_map.find(topico);
  MQTTAsync_messageArrived *callback = iter != client._callback_map.end() ? iter->second : NULL;
  ReleaseSRWLockShared(&client._callback_map_slock);

  int ret = 1;
  if (callback) ret = callback(op, topico, topico_sz, msg);
  if (ret == 1) {
    _dll.freeMessage(&msg);
    _dll.free(topico);
  }

  return ret;
}



void MQTTClient::Subscribe::falha(void *ctx, MQTTAsync_failureData *res) {
  Subscribe &subscribe = *static_cast<Subscribe *>(ctx);
  
  const char *msg = "";
  if (res->message) msg = res->message;
  
  std::cerr << "MQTTClient::Subscribe::falha: Falha ao realizar subscribes. Código: " << res->code << ", mensagem: " << msg << '\n';
  
  subscribe._res = false;
  subscribe.notificar_um();
}



void MQTTClient::Subscribe::sucesso(void *ctx, MQTTAsync_successData *res) {
  Subscribe &subscribe = *static_cast<Subscribe *>(ctx);
  const std::vector<int> &qos_requisitados = subscribe._qos_requisitados;
  const std::vector<char *> &topicos = subscribe._topicos;
  MQTTClient &client = subscribe._client;

  int *qos_devolvidos;
  if (qos_requisitados.size() == 1) {
    qos_devolvidos = &res->alt.qos;
  } else {
    qos_devolvidos = res->alt.qosList;
  }

  for (size_t i = 0; i < qos_requisitados.size(); ++i) {
    //                 falha           QoS devolvido < QoS requisitado
    if (qos_devolvidos[i] == 0x80 || qos_devolvidos[i] < qos_requisitados[i]) {
      std::cerr << "MQTTClient::Subscribe::sucesso: Falha ao se inscrever no tópico " << topicos[i] << " no broker " << subscribe._client._broker_uri
                << ". QoS devolvido: " << qos_devolvidos[i] << '\n';
      // tentar realizar unsubscribe
      // todo
      // caso falhe, apenas silenciar
      AcquireSRWLockExclusive(&client._callback_map_slock);
      for (size_t j = 0; j < qos_requisitados.size(); ++j)
        client._callback_map.erase(topicos[j]);
      ReleaseSRWLockExclusive(&client._callback_map_slock);
      subscribe._res = false;
      subscribe.notificar_um();
      return;
    }
  }

  subscribe._res = true;
  subscribe.notificar_um();
}



bool MQTTClient::subscribe(const std::vector<std::string> &topicos, const std::vector<MQTTAsync_messageArrived *> &callbacks) {
  // Processar entradas
  if (topicos.empty()) {
    std::cerr << "MQTTClient::subscribe: o vetor de tópicos está vazio.\n";
    return false;
  }
  std::vector<int> qosRequeridos(topicos.size(), 1); // todos com QoS 1.
  std::vector<char *> topicos_char_p; topicos_char_p.reserve(topicos.size());
  for (size_t i = 0; i < topicos.size(); ++i) topicos_char_p.push_back(const_cast<char *>(topicos[i].data()));
  
  // Configurar operação de subscribe
  Subscribe subscribe(*this, qosRequeridos, topicos_char_p);

  // Adicionar callbacks no mapa
  AcquireSRWLockExclusive(&_callback_map_slock);
  for (size_t i = 0; i < topicos.size(); ++i)
    _callback_map.insert({topicos[i], callbacks[i]});
  ReleaseSRWLockExclusive(&_callback_map_slock);
  
  // Configurar operação de subscribe
  MQTTAsync_responseOptions opts(MQTTAsync_responseOptions_initializer);
  opts.context = &subscribe;
  opts.onFailure = subscribe.falha;
  opts.onSuccess = subscribe.sucesso;
  int ret = _dll.subscribeMany(_client, (int)topicos.size(), topicos_char_p.data(), qosRequeridos.data(), &opts);
  if (ret != MQTTASYNC_SUCCESS) {
    std::cerr << "MQTTClient::subscribe: Falha ao se inscrever em tópicos no broker " << _broker_uri
              << ". Código: " << ret << ", mensagem: " << _dll.strerror(ret) << '\n';
    AcquireSRWLockExclusive(&_callback_map_slock);
    for (size_t i = 0; i < topicos.size(); ++i)
      _callback_map.erase(topicos[i]);
    ReleaseSRWLockExclusive(&_callback_map_slock);
    return false;
  }

  // Esperar a operação terminar
  subscribe.esperar();

  return subscribe.resultado();
}



void MQTTClient::Unsubscribe::falha(void *op, MQTTAsync_failureData *res) {
  Unsubscribe &unsubscribe = *static_cast<Unsubscribe *>(op);
  MQTTClient &client = unsubscribe._client;
  const char *msg = "";
  if (res->message) msg = res->message;
  std::cerr << "MQTTClient::Unsubscribe::falha: Falha ao se desinscrever de tópicos no broker " << client._broker_uri
            << ". Razão: " << msg;

  unsubscribe._res = false;
  unsubscribe.notificar_um();
}



void MQTTClient::Unsubscribe::sucesso(void *op, MQTTAsync_successData *) {
  Unsubscribe &unsubscribe = *static_cast<Unsubscribe *>(op);

  unsubscribe._res = true;
  unsubscribe.notificar_um();
}



bool MQTTClient::unsubscribe(const std::vector<std::string> &topicos) {
  // Processar entrada:
  std::vector<char *> topicos_char_p; topicos_char_p.reserve(topicos.size());
  for (size_t i = 0; i < topicos.size(); ++i) topicos_char_p.push_back(const_cast<char *>(topicos[i].data()));

  // Configurar operação de unsubscribe
  Unsubscribe unsubscribe(*this, topicos_char_p);

  // Configurar chamada de unsubscribeMany
  MQTTAsync_responseOptions opts(MQTTAsync_responseOptions_initializer);
  opts.context = &unsubscribe;
  opts.onFailure = unsubscribe.falha;
  opts.onSuccess = unsubscribe.sucesso;
  int ret = _dll.unsubscribeMany(_client, (int)topicos_char_p.size(), topicos_char_p.data(), &opts);
  if (ret != MQTTASYNC_SUCCESS) {
    std::cerr << "MQTTClient::unsubscribe: Falha ao se desinscrever em tópicos no broker " << _broker_uri
              << ". Código: " << ret << ", mensagem: " << _dll.strerror(ret) << '\n';
    return false;
  }

  unsubscribe.esperar();

  // Independente do resultado, retirar os tópicos do hashmap.
  AcquireSRWLockExclusive(&_callback_map_slock);
  for (size_t i = 0; i < topicos.size(); ++i) _callback_map.erase(topicos[i]);
  ReleaseSRWLockExclusive(&_callback_map_slock);

  return unsubscribe.resultado();
}