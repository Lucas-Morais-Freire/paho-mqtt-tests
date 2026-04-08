#include <iostream>
#include <MQTTDll/MQTTDll.h>
#include <string>
#include <condition_variable>

MQTTDll mqttdll;



bool connecting;
std::condition_variable connecting_cv;
std::mutex connecting_mtx;

void onConnectSuccess(void *, MQTTAsync_successData *) {
  std::lock_guard<std::mutex> lock(connecting_mtx);
  connecting = false;
  connecting_cv.notify_one();
}

void onConnectFailure(void *, MQTTAsync_failureData *) {
  std::lock_guard<std::mutex> lock(connecting_mtx);
  connecting = false;
  connecting_cv.notify_one();
}



void onSubscribeSuccess(void *, MQTTAsync_successData *) {
  
}



int onMessageArrived(void *, char *topico, int, MQTTAsync_message *msg) {
  auto retval = [&msg, &topico](int retval) {
    mqttdll.freeMessage(&msg);
    mqttdll.free(topico);
    return retval;
  };

  if (msg->dup) return retval(1);
  std::string payload{(char*)msg->payload, static_cast<size_t>(msg->payloadlen)};

  std::cout << payload << '\n';

  return retval(1);
}

int main() {
  if (!mqttdll.loaded()) {
    std::cerr << "dll MQTT não foi carregada.\n";
    return -1;
  }

  MQTTAsync client;
  mqttdll.create(&client, "tcp://172.30.1.25:1883", "mqtt-test", MQTTCLIENT_PERSISTENCE_NONE, NULL);

  MQTTAsync_connectOptions connect_opts = MQTTAsync_connectOptions_initializer;
  connect_opts.keepAliveInterval = 20;
  connect_opts.cleansession = 1;
  connect_opts.connectTimeout = 10;
  connect_opts.retryInterval = 1;
  connect_opts.onSuccess = onConnectSuccess;
  connect_opts.onFailure = onConnectFailure;
  mqttdll.setCallbacks(client, NULL, NULL, onMessageArrived, NULL);
  connecting = true;
  mqttdll.connect(client, &connect_opts);

  {std::unique_lock<std::mutex> lock(connecting_mtx);
    connecting_cv.wait(lock, []{ return !connecting; });
  }

  if (!mqttdll.isConnected(client)) {
    std::cerr << "Não foi possível conectar ao host.\n";
    mqttdll.destroy(&client);
    return -1;
  }

  MQTTAsync_responseOptions response_opts = MQTTAsync_responseOptions_initializer;
  response_opts.onFailure = onSubscribeFailure;
  response_opts.onSuccess = onSubscribeSuccess;
  mqttdll.subscribe(client, "STATUS/RAMPA_R_0", 1, );
  // todo

  return 0;
}