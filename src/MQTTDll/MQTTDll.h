#pragma once

#include <windows.h>
#include "MQTTAsync.h"
#include <stdexcept>

class MQTTDll {
private:
  // Ponteiros para as funções
  int         (*fptr_create)      (MQTTAsync*, const char*, const char*, int, void*) = NULL;
  void        (*fptr_destroy)     (MQTTAsync *) = NULL;
  int         (*fptr_connect)     (MQTTAsync, const MQTTAsync_connectOptions *) = NULL;
  int         (*fptr_isConnected) (MQTTAsync) = NULL;
  int         (*fptr_disconnect)  (MQTTAsync, const MQTTAsync_disconnectOptions *) = NULL;
  int         (*fptr_setCallbacks)(MQTTAsync, void *, MQTTAsync_connectionLost *, MQTTAsync_messageArrived *, MQTTAsync_deliveryComplete *) = NULL;
  int         (*fptr_subscribe)   (MQTTAsync, const char *, int, MQTTAsync_responseOptions *) = NULL;
  int         (*fptr_unsubscribe) (MQTTAsync, const char *, MQTTAsync_responseOptions *) = NULL;
  int         (*fptr_sendMessage) (MQTTAsync, const char *, const MQTTAsync_message *, MQTTAsync_responseOptions *) = NULL;
  void        (*fptr_freeMessage) (MQTTAsync_message **) = NULL;
  void        (*fptr_free)        (void *) = NULL;
  const char *(*fptr_strerror)    (int) = NULL;

  HINSTANCE hDll = NULL;

public:
  explicit MQTTDll() noexcept = default;

  void load() noexcept;

  inline bool loaded() const noexcept { return hDll != NULL; }

  inline int create(MQTTAsync* handle, const char* serverURI, const char* clientId, int persistence_type, void* persistence_context) const noexcept {
    return fptr_create(handle, serverURI, clientId, persistence_type, persistence_context);
  }

  inline void destroy(MQTTAsync *handle) const noexcept {
    return fptr_destroy(handle);
  }

  inline int connect(MQTTAsync handle, const MQTTAsync_connectOptions *options) const noexcept {
    return fptr_connect(handle, options);
  }

  inline int isConnected(MQTTAsync handle) const noexcept {
    return fptr_isConnected(handle);
  }

  inline int disconnect(MQTTAsync handle, const MQTTAsync_disconnectOptions *options) const noexcept {
    return fptr_disconnect(handle, options);
  }

  inline int setCallbacks(MQTTAsync handle, void *context, MQTTAsync_connectionLost *cl, MQTTAsync_messageArrived *ma, MQTTAsync_deliveryComplete *dc) const noexcept {
    return fptr_setCallbacks(handle, context, cl, ma, dc);
  }

  inline int subscribe(MQTTAsync handle, const char *topic, int  qos, MQTTAsync_responseOptions *response) const noexcept {
    return fptr_subscribe(handle, topic, qos, response);
  }

  inline int unsubscribe(MQTTAsync handle, const char *topic, MQTTAsync_responseOptions *response) const noexcept {
    return fptr_unsubscribe(handle, topic, response);
  }

  inline int sendMessage(MQTTAsync handle, const char *destinationName, const MQTTAsync_message *msg, MQTTAsync_responseOptions *response) const noexcept {
    return fptr_sendMessage(handle, destinationName, msg, response);
  }

  inline void freeMessage(MQTTAsync_message **msg) const noexcept {
    return fptr_freeMessage(msg);
  }

  inline void free(void *ptr) const noexcept {
    return fptr_free(ptr);
  }

  inline const char *strerror(int code) const noexcept {
    return fptr_strerror(code);
  }
};