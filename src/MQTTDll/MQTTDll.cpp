#include "MQTTDll.h"

#include <stdexcept>

void MQTTDll::load() noexcept {
  hDll = LoadLibrary("libpaho-mqtt3a.dll");
  if (hDll == NULL) return;

  fptr_create = (int (*)(MQTTAsync *, const char*, const char*, int, void*))(void *)
    GetProcAddress(hDll, "MQTTAsync_create");

  fptr_destroy = (void (*)(MQTTAsync *))(void *)
    GetProcAddress(hDll, "MQTTAsync_destroy");

  fptr_connect = (int (*)(MQTTAsync, const MQTTAsync_connectOptions *))(void *)
    GetProcAddress(hDll, "MQTTAsync_connect");
    
  fptr_isConnected = (int (*)(MQTTAsync))(void *)
    GetProcAddress(hDll, "MQTTAsync_isConnected");

  fptr_disconnect = (int (*)(MQTTAsync, const MQTTAsync_disconnectOptions *))(void *)
    GetProcAddress(hDll, "MQTTAsync_disconnect");

  fptr_setCallbacks = (int (*)(MQTTAsync, void *, MQTTAsync_connectionLost *, MQTTAsync_messageArrived *, MQTTAsync_deliveryComplete *))(void *)
    GetProcAddress(hDll, "MQTTAsync_setCallbacks");

  fptr_subscribe = (int (*)(MQTTAsync, const char *, int, MQTTAsync_responseOptions *))(void *)
    GetProcAddress(hDll, "MQTTAsync_subscribe");

  fptr_unsubscribe = (int (*)(MQTTAsync, const char *, MQTTAsync_responseOptions *))(void *)
    GetProcAddress(hDll, "MQTTAsync_unsubscribe");

  fptr_sendMessage = (int (*)(MQTTAsync, const char *, const MQTTAsync_message *, MQTTAsync_responseOptions *))(void *)
    GetProcAddress(hDll, "MQTTAsync_sendMessage");

  fptr_freeMessage = (void (*)(MQTTAsync_message **))(void *)
    GetProcAddress(hDll, "MQTTAsync_freeMessage");

  fptr_free = (void (*)(void *))(void *)
    GetProcAddress(hDll, "MQTTAsync_free");

  fptr_strerror = (const char *(*)(int))(void *)
    GetProcAddress(hDll, "MQTTAsync_strerror");
}
