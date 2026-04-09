#pragma once

#include <MQTTDll/MQTTDll.h>
#include <condition_variable>

class MQTT {
private:
  // lib
  MQTTDll dll;

  // Dados do client MQTT
  MQTTAsync client;
  std::string brokerURI;
  std::string clientId;

  // Utilidades para conexão
  static const int KEEPALIVE_INTERVAL = 20;
  static const int CLEANSESSION = 1;
  static const int CONNECT_TOUT = 10;
  static const int RETRY_INTERVAL = 3;
  bool conectando = false;
  std::condition_variable conectando_cv;
  std::mutex conectando_mtx;
  static void conexaoFalhouCallback(void *client, MQTTAsync_failureData *res);
  static void conexaoEstabelecidaCallback(void *client, MQTTAsync_successData *res);
  static void conexaoPerdidaCallback(void* client, char* cause);

  // Utilidades para inscrição

public:
  explicit MQTT(const std::string &brokerURI, const std::string &clientId);
  
  bool conectar();

  bool conectado();

  void subscribe();
};