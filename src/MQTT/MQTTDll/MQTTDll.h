#pragma once

#include <windows.h>
#include "MQTTAsync.h"
#include <stdexcept>

class MQTTDll {
private:
  // Ponteiros para as funções
  int         (*fptr_create)         (MQTTAsync *, const char*, const char*, int, void*) = NULL;
  void        (*fptr_destroy)        (MQTTAsync *) = NULL;
  int         (*fptr_connect)        (MQTTAsync, const MQTTAsync_connectOptions *) = NULL;
  int         (*fptr_reconnect)      (MQTTAsync) = NULL;
  int         (*fptr_isConnected)    (MQTTAsync) = NULL;
  int         (*fptr_disconnect)     (MQTTAsync, const MQTTAsync_disconnectOptions *) = NULL;
  int         (*fptr_setCallbacks)   (MQTTAsync, void *, MQTTAsync_connectionLost *, MQTTAsync_messageArrived *, MQTTAsync_deliveryComplete *) = NULL;
  int         (*fptr_setConnected)   (MQTTAsync, void *, MQTTAsync_connected *) = NULL;
  int         (*fptr_subscribe)      (MQTTAsync, const char *, int, MQTTAsync_responseOptions *) = NULL;
  int         (*fptr_subscribeMany)  (MQTTAsync, int, char *const *, const int *, MQTTAsync_responseOptions *) = NULL;
  int         (*fptr_unsubscribe)    (MQTTAsync, const char *, MQTTAsync_responseOptions *) = NULL;
  int         (*fptr_unsubscribeMany)(MQTTAsync, int, char *const *, MQTTAsync_responseOptions *) = NULL;
  int         (*fptr_sendMessage)    (MQTTAsync, const char *, const MQTTAsync_message *, MQTTAsync_responseOptions *) = NULL;
  void        (*fptr_freeMessage)    (MQTTAsync_message **) = NULL;
  void        (*fptr_free)           (void *) = NULL;
  static const char *defaultErrMsg(int) {
    return "MQTTDll: função não carregada.";
  }
  const char *(*fptr_strerror) (int) = defaultErrMsg;

  HINSTANCE hDll = NULL;

public:
  explicit MQTTDll() noexcept {};

  bool load() noexcept;

  inline int create(MQTTAsync* handle, const char* serverURI, const char* clientId, int persistence_type, void* persistence_context) const noexcept {
    return fptr_create ? fptr_create(handle, serverURI, clientId, persistence_type, persistence_context) : MQTTASYNC_FAILURE;
  }

  inline void destroy(MQTTAsync *handle) const noexcept {
    if (fptr_destroy) fptr_destroy(handle);
  }

  inline int connect(MQTTAsync handle, const MQTTAsync_connectOptions *options) const noexcept {
    return fptr_connect ? fptr_connect(handle, options) : MQTTASYNC_FAILURE;
  }

  inline int reconnect(MQTTAsync handle) const noexcept {
    return fptr_reconnect ? fptr_reconnect(handle) : MQTTASYNC_FAILURE;
  }

  inline int isConnected(MQTTAsync handle) const noexcept {
    return fptr_isConnected ? fptr_isConnected(handle) : 0;
  }

  inline int disconnect(MQTTAsync handle, const MQTTAsync_disconnectOptions *options) const noexcept {
    return fptr_disconnect ? fptr_disconnect(handle, options) : MQTTASYNC_FAILURE;
  }

  inline int setCallbacks(MQTTAsync handle, void *context, MQTTAsync_connectionLost *cl, MQTTAsync_messageArrived *ma, MQTTAsync_deliveryComplete *dc) const noexcept {
    return fptr_setCallbacks ? fptr_setCallbacks(handle, context, cl, ma, dc) : MQTTASYNC_FAILURE;
  }

  inline int setConnected(MQTTAsync handle, void *context, MQTTAsync_connected *co) const noexcept {
    return fptr_setConnected ? fptr_setConnected(handle, context, co) : MQTTASYNC_FAILURE;
  }

  inline int subscribe(MQTTAsync handle, const char *topic, int  qos, MQTTAsync_responseOptions *response) const noexcept {
    return fptr_subscribe ? fptr_subscribe(handle, topic, qos, response) : MQTTASYNC_FAILURE;
  }

  inline int subscribeMany(MQTTAsync handle, int count, char *const *topic, const int *qos, MQTTAsync_responseOptions *response) const noexcept {
    return fptr_subscribeMany ? fptr_subscribeMany(handle, count, topic, qos, response) : MQTTASYNC_FAILURE;
  }

  inline int unsubscribe(MQTTAsync handle, const char *topic, MQTTAsync_responseOptions *response) const noexcept {
    return fptr_unsubscribe ? fptr_unsubscribe(handle, topic, response) : MQTTASYNC_FAILURE;
  }

  inline int unsubscribeMany(MQTTAsync handle, int count, char *const *topic, MQTTAsync_responseOptions *response) const noexcept {
    return fptr_unsubscribeMany ? fptr_unsubscribeMany(handle, count, topic, response) : MQTTASYNC_FAILURE;
  }

  inline int sendMessage(MQTTAsync handle, const char *destinationName, const MQTTAsync_message *msg, MQTTAsync_responseOptions *response) const noexcept {
    return fptr_sendMessage ? fptr_sendMessage(handle, destinationName, msg, response) : MQTTASYNC_FAILURE;
  }

  /**
   * @brief This function frees memory allocated to an MQTT message, including the additional memory allocated to the message payload.
   * The client application calls this function when the message has been fully processed.
   * @param msg The address of a pointer to the `MQTTAsync_message` structure to be freed.
   * @note This function does not free the memory allocated to a message topic string. It is the responsibility of the client application
   * to free this memory using the `free(void *)` library function.
   */
  inline void freeMessage(MQTTAsync_message **msg) const noexcept {
    if (fptr_freeMessage) fptr_freeMessage(msg);
  }

  inline void free(void *ptr) const noexcept {
    if (fptr_free) fptr_free(ptr);
  }

  inline const char *strerror(int code) const noexcept {
    return fptr_strerror(code);
  }
};