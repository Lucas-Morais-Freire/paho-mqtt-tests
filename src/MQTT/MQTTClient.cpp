#include "MQTTClient.h"

#include <iostream>
#include <atomic>

std::mutex MQTTClient::_dll_mtx;
MQTTDll MQTTClient::_dll;

MQTTClient::MQTTClient(const std::string &broker_uri, const std::string &client_id) noexcept :
_broker_uri(broker_uri),
_client_id(client_id),
_conexao(this),
_desconexao(this) {
  {std::lock_guard<std::mutex> lock(_dll_mtx);
    if (!_dll.load()) std::cerr << "MQTTClient::MQTTClient: libpaho-mqtt3a.dll não foi carregada\n";
  }

  int ret = _dll.create(&_handle, broker_uri.c_str(), client_id.c_str(), MQTTCLIENT_PERSISTENCE_NONE, NULL);
  if (ret != MQTTASYNC_SUCCESS) {
    std::cout << "MQTTClient::MQTTClient: Não foi possível criar client MQTTAsync: " << _dll.strerror(ret) << '\n';
  }
}



MQTTClient::~MQTTClient() noexcept {
  if (conectado()) desconectar();

  _dll.destroy(&_handle);
}



// --- MQTTClient::Contexto

bool MQTTClient::Contexto::esperar() {
  std::unique_lock<std::mutex> lock(_finalizada_mtx);
  _finalizada_cv.wait(lock, [this]{ return _finalizada; });
  _finalizada = false;
  // Como temos uma barreira de memória devido ao mutex `_finalizada_mtx`, o valor de `_res` é consistente.
  return _res;
}


void MQTTClient::Contexto::notificar_um() {
  std::lock_guard<std::mutex> lock(_finalizada_mtx);
  _finalizada = true;
  _finalizada_cv.notify_one();
}



// --- CONNECT

MQTTClient::Connect::Connect(MQTTClient *client) noexcept :
Contexto(client) {
  _opts.context = this;
  _opts.onFailure = falha;
  _opts.onSuccess = sucesso;
  _opts.keepAliveInterval = _KEEPALIVE_INTERVAL; // KeepAlive TCP
  _opts.cleansession = _CLEANSESSION;            // Todas as vezes que uma nova conexão for estabelecida, a sessão será resetada
  _opts.connectTimeout = _CONNECT_TOUT;          // Timout da tentativa de conexão
  _opts.retryInterval = _RETRY_INTERVAL;         // Intervalo de tentativas de envio de mensagens não reconhecidas.
}

void MQTTClient::Connect::falha(void *ctx, MQTTAsync_failureData *res) {
  Connect &conexao = *static_cast<Connect *>(ctx);
  MQTTClient &client = conexao._client;
  
  std::string msg = std::string("MQTTClient::Connect::falha: não foi possível estabelecer uma conexão com o broker em ") + client._broker_uri + '.';
  if (res) {
    msg += " Código: " + std::to_string(res->code);
    if (res->message) msg += std::string(", mensagem: ") + res->message;
  }
  msg += '\n';
  std::cerr << msg;

  conexao._res = false;
  conexao.notificar_um();
}



void MQTTClient::Connect::sucesso(void *ctx, MQTTAsync_successData *res) {
  Connect &conexao = *static_cast<Connect *>(ctx);
  MQTTClient &client = conexao._client;

  std::cout << "MQTTClient::Connect::sucesso: conexão ao broker em " + client._broker_uri + " bem-sucedida.\n";

  conexao._sessao_presente = res->alt.connect.sessionPresent;
  if (!conexao._sessao_presente) {
    AcquireSRWLockExclusive(&client._callback_map_slock);
    client._callback_map.clear();
    ReleaseSRWLockExclusive(&client._callback_map_slock);
  }
  conexao._res = true;
  conexao._primeira_conexao = false;
  conexao.notificar_um();
}



void MQTTClient::Connect::perdida(void *ctx, char *cause) {
  MQTTClient &client = *static_cast<MQTTClient *>(ctx);
  std::string msg = std::string("MQTTClient::conexaoPerdida: Conexão com o broker ") + client._broker_uri + " perdida. Causa: " + cause + '\n';

  std::cerr << msg;
}



int MQTTClient::Connect::mensagemRecebida(void *ctx, char *topico, int topico_sz, MQTTAsync_message *msg) {
  MQTTClient &client = static_cast<Connect *>(ctx)->_client;

  AcquireSRWLockShared(&client._callback_map_slock);
  std::unordered_map<std::string, MQTTAsync_messageArrived *>::iterator iter = client._callback_map.find(topico);
  MQTTAsync_messageArrived *callback = iter != client._callback_map.end() ? iter->second : NULL;
  ReleaseSRWLockShared(&client._callback_map_slock);

  int ret = 1;
  if (callback) ret = callback(ctx, topico, topico_sz, msg);
  if (ret == 1) {
    _dll.freeMessage(&msg);
    _dll.free(topico);
  }

  return ret;
}



bool MQTTClient::Connect::despachar() {
  
  // Não permite mais de uma thread de tentar se (des)conectar ao mesmo tempo.
  std::lock_guard<std::mutex> lock(_client._conexao_mtx);
  if (_client.conectado()) return true;

  if (_primeira_conexao) {
    int ret = _dll.setCallbacks(_client._handle, this, perdida, mensagemRecebida, NULL);
    if (ret != MQTTASYNC_SUCCESS) {
      std::cerr << std::string("MQTTClient::Connect::despachar: Erro em setCallbacks(): ") + _dll.strerror(ret) + '\n';
      return false;
    }
  }

  int ret = _dll.connect(_client._handle, &_opts);
  if (ret != MQTTASYNC_SUCCESS) {
    std::cerr << std::string("MQTTClient::conectar: Erro em connect(): ") + _dll.strerror(ret) + '\n';
    return false;
  }

  return esperar();
}



// SUBSCRIBE

MQTTClient::Subscribe::Subscribe(MQTTClient *client, const std::vector<std::string> &topicos, const std::vector<MQTTAsync_messageArrived *> &callbacks) noexcept :
Contexto(client), _topicos(topicos), _callbacks(callbacks) {
  _opts.context = this;
  _opts.onFailure = falha;
  _opts.onSuccess = sucesso;
}



bool MQTTClient::Subscribe::despachar() {
  if (_topicos.empty()) {
    std::cerr << "MQTTClient::Subscribe::despachar: O vetor de tópicos está vazio.\n";
    return false;
  }

  std::vector<char *> topicos_char_p; topicos_char_p.reserve(_topicos.size());
  for (size_t i = 0; i < _topicos.size(); ++i) topicos_char_p.push_back(const_cast<char *>(_topicos[i].data()));

  AcquireSRWLockExclusive(&_client._callback_map_slock);
  for (size_t i = 0; i < _topicos.size(); ++i) _client._callback_map.insert({_topicos[i], _callbacks[i]});
  ReleaseSRWLockExclusive(&_client._callback_map_slock);

  int ret = _dll.subscribeMany(_client._handle, (int)_topicos.size(), topicos_char_p.data(), _qos_requisitados.data(), &_opts);
  if (ret != MQTTASYNC_SUCCESS) {
    std::string msg = std::string("MQTTClient::Subscribe::despachar: Falha ao se inscrever em tópicos no broker ") + _client._broker_uri +
                      ". Código: " + std::to_string(ret) + ", mensagem: " + _dll.strerror(ret) + '\n';
    AcquireSRWLockExclusive(&_client._callback_map_slock);
    for (size_t j = 0; j < _qos_requisitados.size(); ++j) _client._callback_map.erase(_topicos[j]);
    ReleaseSRWLockExclusive(&_client._callback_map_slock);

    return false;
  }

  return esperar();
}



void MQTTClient::Subscribe::falha(void *ctx, MQTTAsync_failureData *res) {
  Subscribe &subscribe = *static_cast<Subscribe *>(ctx);
  MQTTClient &client = subscribe._client;
  
  std::string msg = std::string("MQTTClient::Subscribe::falha: Falha ao realizar subscribes no broker em ") + client._broker_uri + '.';
  if (res) {
    msg += " Código: " + std::to_string(res->code);
    if (res->message) msg += std::string(", mensagem: ") + res->message;
  }
  msg += '\n';
  std::cerr << msg;

  // Remover os tópicos do hashmap.
  AcquireSRWLockExclusive(&client._callback_map_slock);
  for (size_t j = 0; j < subscribe._qos_requisitados.size(); ++j) client._callback_map.erase(subscribe._topicos[j]);
  ReleaseSRWLockExclusive(&client._callback_map_slock);
  
  subscribe._res = false;
  subscribe.notificar_um();
}



void MQTTClient::Subscribe::sucesso(void *ctx, MQTTAsync_successData *res) {
  Subscribe &subscribe = *static_cast<Subscribe *>(ctx);
  const std::vector<int> &qos_requisitados = subscribe._qos_requisitados;
  const std::vector<std::string> &topicos = subscribe._topicos;
  MQTTClient &client = subscribe._client;

  int *qos_devolvidos;
  if (qos_requisitados.size() == 1) {
    qos_devolvidos = &res->alt.qos;
  } else {
    qos_devolvidos = res->alt.qosList;
  }

  for (size_t i = 0; i < qos_requisitados.size(); ++i) {
    //                 falha           QoS devolvido < QoS requisitado
    if (qos_devolvidos[i] == MQTT_BAD_SUBSCRIBE || qos_devolvidos[i] < qos_requisitados[i]) {
      std::string msg = std::string("MQTTClient::Subscribe::sucesso: Falha ao se inscrever no tópico ") + topicos[i] + " no broker " + client._broker_uri +
                        ". QoS devolvido: " + std::to_string(qos_devolvidos[i]) + '\n';
      std::cerr << msg;
      // tentar realizar unsubscribe
      // todo
      // caso falhe, apenas remover os callbacks do hashmap
      AcquireSRWLockExclusive(&client._callback_map_slock);
      for (size_t j = 0; j < subscribe._qos_requisitados.size(); ++j) client._callback_map.erase(subscribe._topicos[j]);
      ReleaseSRWLockExclusive(&client._callback_map_slock);
      subscribe._res = false;
      subscribe.notificar_um();
      return;
    }
  }

  subscribe._res = true;
  subscribe.notificar_um();
}


// UNSUBSCRIBE

MQTTClient::Unsubscribe::Unsubscribe(MQTTClient *client, const std::vector<std::string> &topicos) noexcept :
Contexto(client), _topicos(topicos) {
  _opts.context = this;
  _opts.onFailure = falha;
  _opts.onSuccess = sucesso;
}



bool MQTTClient::Unsubscribe::despachar() {
  std::vector<char *> topicos_char_p; topicos_char_p.reserve(_topicos.size());
  for (size_t i = 0; i < _topicos.size(); ++i) topicos_char_p.push_back(const_cast<char *>(_topicos[i].data()));

  int ret = _dll.unsubscribeMany(_client._handle, (int)_topicos.size(), topicos_char_p.data(), &_opts);
  if (ret != MQTTASYNC_SUCCESS) {
    std::string msg = std::string("MQTTClient::unsubscribe: Falha ao se desinscrever em tópicos no broker ") + _client._broker_uri +
                      ". Código: " + std::to_string(ret) + ", mensagem: " + _dll.strerror(ret) + '\n';
    std::cerr << msg;
    return false;
  }

  AcquireSRWLockExclusive(&_client._callback_map_slock);
  for (size_t i = 0; i < _topicos.size(); ++i) _client._callback_map.erase(_topicos[i]);
  ReleaseSRWLockExclusive(&_client._callback_map_slock);

  return esperar();
}



void MQTTClient::Unsubscribe::falha(void *ctx, MQTTAsync_failureData *res) {
  Unsubscribe &unsubscribe = *static_cast<Unsubscribe *>(ctx);
  MQTTClient &client = unsubscribe._client;

  std::string msg = std::string("MQTTClient::Unsubscribe::falha: Falha ao se desinscrever de tópicos no broker ") + client._broker_uri + '.';
  if (res) {
    msg += " Código: " + std::to_string(res->code);
    if (res->message) msg += std::string(", mensagem: ") + res->message;
  }
  msg += '\n';
  std::cerr << msg;

  unsubscribe._res = false;
  unsubscribe.notificar_um();
}



void MQTTClient::Unsubscribe::sucesso(void *ctx, MQTTAsync_successData *) {
  Unsubscribe &unsubscribe = *static_cast<Unsubscribe *>(ctx);

  unsubscribe._res = true;
  unsubscribe.notificar_um();
}



// DISCONNECT

MQTTClient::Disconnect::Disconnect(MQTTClient *client) noexcept : Contexto(client) {
  _opts.context = this;
  _opts.onFailure = falha;
  _opts.onSuccess = sucesso;
  _opts.timeout =_DISCONNECT_TOUT;
}



bool MQTTClient::Disconnect::despachar() {
  if (!_client.conectado()) return true;

  std::lock_guard<std::mutex> lock(_client._conexao_mtx);
  int ret = _dll.disconnect(_client._handle, &_opts);
  if (ret != MQTTASYNC_SUCCESS) {
    std::string msg = std::string("MQTTClient::Disconnect::despachar: Erro ao desconectar do broker em ") + _client._broker_uri +
                      ". Código:" + std::to_string(ret) + ", mensagem: " + _dll.strerror(ret) + '\n';
    std::cerr << msg;
    return false;
  }

  // Não apagar o hashmap de callbacks pois em uma próxima sessão podemos recuperar as inscrições.
  return esperar();
}



void MQTTClient::Disconnect::falha(void *ctx, MQTTAsync_failureData *res) {
  Disconnect &disconnect = *static_cast<Disconnect *>(ctx);
  MQTTClient &client = disconnect._client;

  std::string msg = std::string("MQTTClient::Disconnect::falha: Erro ao tentar se desconectar do broker em ") + client._broker_uri + '.';
  if (res) {
    msg += " Código: " + std::to_string(res->code);
    if (res->message) msg += std::string(", mensagem: ") + res->message;
  }
  msg += '\n';
  std::cerr << msg;

  disconnect._res = false;
  disconnect.notificar_um();
}



void MQTTClient::Disconnect::sucesso(void *ctx, MQTTAsync_successData *) {
  Disconnect &disconnect = *static_cast<Disconnect *>(ctx);
  MQTTClient &client = disconnect._client;

  std::cout << "MQTTClient::Disconnect::sucesso: Desconectado de " + client._broker_uri + '\n';

  disconnect._res = true;
  disconnect.notificar_um();
}
