#include "MQTT.h"

#include <iostream>
#include <stdexcept>

MQTT::MQTT(const std::string &brokerURI, const std::string &clientId) :
brokerURI(brokerURI),
clientId(clientId) {
  dll.load();
  if (!dll.loaded()) return;

  int ret = dll.create(&client, brokerURI.c_str(), clientId.c_str(), MQTTCLIENT_PERSISTENCE_NONE, NULL);
  if (ret != MQTTASYNC_SUCCESS) {
    std::cout << "MQTT::MQTT: Não foi possível criar client MQTT: " << dll.strerror(ret) << '\n';
  }
}

void MQTT::conexaoFalhouCallback(void *client, MQTTAsync_failureData *res)
{
  MQTT &This = *static_cast<MQTT *>(client);
  const char *msg = "";
  if (res->message) msg = res->message;
  std::cerr << "MQTT::onFail: não foi possível estabelecer uma conexão com o broker em "<< This.brokerURI << ". Código: " << res->code << ", mensagem: " << res->message << '\n';

  std::lock_guard<std::mutex> lock(This.conectando_mtx);
  This.conectando = false;
  This.conectando_cv.notify_one();
}



void MQTT::conexaoEstabelecidaCallback(void *client, MQTTAsync_successData *res) {
  MQTT &This = *static_cast<MQTT *>(client);
  std::cout << "MQTT::onSuccess: conexão ao broker em " << This.brokerURI << " bem-sucedida.\n";

  std::lock_guard<std::mutex> lock(This.conectando_mtx);
  This.conectando = false;
  This.conectando_cv.notify_one();
}



void MQTT::conexaoPerdidaCallback(void *client, char *cause) {
  MQTT &This = *static_cast<MQTT *>(client);

  std::cerr << "MQTT::conexaoPerdidaCallback: Conexão com o broker perdida. Razão: " << cause << '\n';
}



bool MQTT::conectar() {
  MQTTAsync_connectOptions opts = MQTTAsync_connectOptions_initializer;
  opts.keepAliveInterval = KEEPALIVE_INTERVAL;
  opts.cleansession = CLEANSESSION;
  opts.connectTimeout = CONNECT_TOUT;
  opts.retryInterval = RETRY_INTERVAL;
  opts.onFailure = conexaoFalhouCallback;
  opts.onSuccess = conexaoEstabelecidaCallback;

  int ret = dll.setCallbacks(client, this, conexaoPerdidaCallback, NULL, NULL);
  if (ret != MQTTASYNC_SUCCESS) {
    std::cerr << "MQTT::conectar: Erro ao adicionar callbacks: " << dll.strerror(ret) << '\n';
    return false;
  }

  conectando = true;
  ret = dll.connect(client, &opts);
  if (ret != MQTTASYNC_SUCCESS) {
    conectando = false;
    std::cerr << "MQTT::conectar: Erro ao conectar: " << dll.strerror(ret) << '\n';
    return false;
  }
  {std::unique_lock<std::mutex> lock(conectando_mtx);
    conectando_cv.wait(lock, [this]{ return !conectando; });
  }

  return dll.isConnected(client);
}
