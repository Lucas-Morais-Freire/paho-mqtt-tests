#include <iostream>
#include <MQTT/MQTTClient.h>
#include <string>
#include <vector>
#include <thread>

#include <mqtt-test.h>

int mensagemRecebida(void *, char *topico, int, MQTTAsync_message *msg) {
  std::string res{topico};
  char * payload = static_cast<char *>(msg->payload);
  res = res + ": " + payload + '\n';
  std::cout << res;

  return 1;
}



int main([[maybe_unused]] int argc, [[maybe_unused]] char** argv) {
  MQTTClient client("tcp://172.30.1.25", "mqtt-test");
  while (true) {

    if (!client.conectar()) {
      std::cerr << "Falha ao conectar ao broker.\n";
      return -1;
    }

    if (!client.subscribe(
      std::vector<std::string>{"STATUS/RAMPA_R_0", "STATUS/BOOLEANO_RW_0", "STATUS/INTEIRO_RW_0"},
      std::vector<MQTTAsync_messageArrived *>(3, mensagemRecebida)
    )) {
      std::cerr << "Falha no subscribe\n";
      return -1;
    }
    
    if (!client.unsubscribe(std::vector<std::string>{"STATUS/RAMPA_R_0", "STATUS/BOOLEANO_RW_0"})) {
      std::cerr << "Falha no unsubscribe\n";
      return -1;
    }

    client.desconectar();

  }
  return 0;
}