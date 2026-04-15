#include <iostream>
#include <MQTT/MQTTClient.h>
#include <string>
#include <vector>
#include <thread>

#include <mqtt-test.h>

int mensagemRecebida(void *, char *topico, int, MQTTAsync_message *msg) {
  std::string res{topico};
  res = res + ": " + std::string((const char*)msg->payload, msg->payloadlen) + '\n';
  std::cout << res;

  return 1;
}

int mensagemTruncada(void *, char *topico, int, MQTTAsync_message *msg) {
  std::string res{topico};
  std::string payload((const char*)msg->payload, msg->payloadlen);
  payload.resize(payload.size()/2); 
  res += std::string(": ") + payload + '\n';
  std::cout << res;

  return 1;
}



int main([[maybe_unused]] int argc, [[maybe_unused]] char** argv) {
  MQTTClient client("tcp://127.0.0.1", "mqtt-test");

  for (size_t i = 0; i < 100; i++) {
    if (!client.conectar()) {
      std::cerr << "Falha ao conectar ao broker.\n";
      return -1;
    }

    if (!client.subscribe(
      std::vector<const char *>{"esp32/topic1", "esp32/topic2", "esp32/topic3"},
      std::vector<MQTTAsync_messageArrived *>{mensagemRecebida, mensagemTruncada, mensagemRecebida}
    )) {
      std::cerr << "Falha no subscribe\n";
      return -1;
    }

    std::this_thread::sleep_for(std::chrono::duration<int64_t>(1));
    
    if (!client.unsubscribe(std::vector<const char *>{"esp32/topic1", "esp32/topic2"})) {
      std::cerr << "Falha no unsubscribe\n";
      return -1;
    }

    std::this_thread::sleep_for(std::chrono::duration<int64_t>(1));
    client.desconectar();
  }
  
  return 0;
}