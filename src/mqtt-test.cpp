#include <iostream>
#include <MQTT/MQTTClient.h>
#include <string>

int main() {
  MQTTClient client("tcp://172.30.1.25", "mqtt-test");

  if (!client.conectar()) {
    std::cerr << "Falha ao conectar ao broker.\n";
    return -1;
  }

  if (!client.subscribe("STATUS/RAMPA_R_0")) {
    std::cerr << "Falha no subscribe\n";
    return -1;
  }

  return 0;
}