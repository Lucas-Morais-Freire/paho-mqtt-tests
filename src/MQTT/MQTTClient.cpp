#include "MQTTClient.h"

#include <iostream>
#include <stdexcept>


// --- MQTTClient::Conexao

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


// --- MQTTClient

MQTTDll MQTTClient::_dll;

MQTTClient::MQTTClient(const std::string &broker_uri, const std::string &client_id) :
_broker_uri(broker_uri),
_client_id(client_id),
_conexao(_broker_uri) {
  {std::lock_guard<std::mutex> lock(_dll_mtx);
    if (!_dll.loaded()) _dll.load();
    if (!_dll.loaded()) throw std::runtime_error("MQTTClient::MQTTClient: libpaho-mqtt3a.dll não pôde ser carregada.");
  }

  int ret = _dll.create(&_client, broker_uri.c_str(), client_id.c_str(), MQTTCLIENT_PERSISTENCE_NONE, NULL);
  if (ret != MQTTASYNC_SUCCESS) {
    std::cout << "MQTTClient::MQTTClient: Não foi possível criar client MQTTClient: " << _dll.strerror(ret) << '\n';
  }
}



void MQTTClient::Conexao::falha(void *ctx, MQTTAsync_failureData *res) {
  Conexao &conexao = *static_cast<Conexao *>(ctx);

  const char *msg = "";
  if (res->message) msg = res->message;
  std::cerr << "MQTTClient::onFail: não foi possível estabelecer uma conexão com o broker em "<< conexao._broker_uri << ". Código: " << res->code << ", mensagem: " << msg << '\n';

  conexao._res = false;
  conexao.notificar_um();
}



void MQTTClient::Conexao::sucesso(void *ctx, MQTTAsync_successData *) {
  Conexao &conexao = *static_cast<Conexao *>(ctx);

  std::cout << "MQTTClient::onSuccess: conexão ao broker em " << conexao._broker_uri << " bem-sucedida.\n";

  conexao._res = true;
  conexao.notificar_um();
}



void MQTTClient::conexaoPerdida(void *ctx, char *cause) {
  MQTTClient &client = *static_cast<MQTTClient *>(ctx);

  std::cerr << "MQTTClient::conexaoPerdidaCallback: Conexão com o broker " << client._broker_uri << " perdida. Causa: " << cause << '\n';
}



int MQTTClient::mensagemRecebidaIgnorar(void *ctx, char *topico, int, MQTTAsync_message *msg) {
  MQTTClient *client = static_cast<MQTTClient *>(ctx);
  client->_dll.free(topico);
  client->_dll.freeMessage(&msg);
  return 1;
}



bool MQTTClient::conectar() {
  // Não permite mais de uma thread de tentar se (des)conectar ao mesmo tempo.
  std::lock_guard<std::mutex> lock(_conexao_mtx);
  if (conectado()) return true;

  // Configurar callbacks iniciais. Por hora, ignorar todas as mensagens recebidas.
  int ret = _dll.setCallbacks(_client, this, conexaoPerdida, mensagemRecebidaIgnorar, NULL);
  if (ret != MQTTASYNC_SUCCESS) {
    std::cerr << "MQTTClient::conectar: Erro ao adicionar callbacks: " << _dll.strerror(ret) << '\n';
    return false;
  }

  // Configurar as opções de conexão.
  MQTTAsync_connectOptions opts = MQTTAsync_connectOptions_initializer;
  opts.context = &_conexao;
  opts.keepAliveInterval = _KEEPALIVE_INTERVAL; // KeepAlive TCP
  opts.cleansession = _CLEANSESSION;            // Todas as vezes que uma nova conexão for estabelecida, a sessão será resetada
  opts.connectTimeout = _CONNECT_TOUT;          // Timout da tentativa de conexão
  opts.retryInterval = _RETRY_INTERVAL;         // Intervalo de tentativas de envio de mensagens não reconhecidas.
  opts.onFailure = Conexao::falha;
  opts.onSuccess = Conexao::sucesso;
  ret = _dll.connect(_client, &opts);
  if (ret != MQTTASYNC_SUCCESS) {
    std::cerr << "MQTTClient::conectar: Erro ao conectar: " << _dll.strerror(ret) << '\n';
    return false;
  }

  // Esperar pela tentativa de conexão terminar.
  _conexao.esperar();

  // Retornar o estado da conexão.
  return _conexao.resultado();
}



void MQTTClient::Subscribe::falha(void *ctx, MQTTAsync_failureData *res) {
  Subscribe &subscribe = *static_cast<Subscribe *>(ctx);
  
  const char *msg = "";
  if (res->message) msg = res->message;
  
  std::cerr << "MQTTClient::falhaSubscribeCallback: Falha ao realizar subscribes. Código: " << res->code << ", mensagem: " << msg << '\n';
  
  subscribe._res = false;
  subscribe.notificar_um();
}



void MQTTClient::Subscribe::sucesso(void *ctx, MQTTAsync_successData *res) {
  Subscribe &subscribe = *static_cast<Subscribe *>(ctx);
  const std::vector<int> &qosRequisitados = subscribe._qosRequisitados;
  const std::vector<char *> &topicos = subscribe._topicos;

  int *qosDevolvidos;
  if (qosRequisitados.size() == 1) {
    qosDevolvidos = &res->alt.qos;
  } else {
    qosDevolvidos = res->alt.qosList;
  }

  for (size_t i = 0; i < qosRequisitados.size(); i++) {
    //                  falha          QoS devolvido < QoS requisitado
    if (qosDevolvidos[i] == 0x80 || qosDevolvidos[i] < qosRequisitados[i]) {
      std::cerr << "MQTTClient::Subscribe::sucesso: Falha ao se inscrever no tópico " << topicos[i] << " no broker " << subscribe._broker_uri
                << ". QoS devolvido: " << qosDevolvidos[i] << '\n';
      subscribe._res = false;
      subscribe.notificar_um();
      return;
    }
  }

  subscribe._res = true;
  subscribe.notificar_um();
}



bool MQTTClient::subscribe(const std::vector<char *> &topicos) {
  if (topicos.empty()) {
    std::cerr << "MQTTClient::subscribe: o vetor de tópicos está vazio.\n";
    return false;
  }

  // Configurar operação
  std::vector<int> qosRequeridos(topicos.size(), 1); // todos com QoS 1.
  Subscribe subscribe(_broker_uri, qosRequeridos, topicos);
  
  // Configurar operação de subscribe
  MQTTAsync_responseOptions opts(MQTTAsync_responseOptions_initializer);
  opts.context = &subscribe;
  opts.onFailure = Subscribe::falha;
  opts.onSuccess = Subscribe::sucesso;
  int ret = _dll.subscribeMany(_client, (int)topicos.size(), topicos.data(), qosRequeridos.data(), &opts);
  if (ret != MQTTASYNC_SUCCESS) {
    std::cerr << "MQTTClient::subscribe: Falha ao se inscrever em tópicos no broker " << _broker_uri
              << ". Código: " << ret << ", mensagem: " << _dll.strerror(ret) << '\n';
  }

  // Esperar a operação terminar
  subscribe.esperar();

  return subscribe.resultado();
}
