#pragma once

#include "MQTTDll/MQTTDll.h"
#include <condition_variable> 
#include <string>
#include <vector>
#include <unordered_map>
#include <windows.h>

class MQTTClient {
private:
  // --- lib
  static std::mutex _dll_mtx;
  static MQTTDll _dll;

  // --- Client da lib paho.mqtt.c
  MQTTAsync _handle = NULL;
  std::string _broker_uri;
  std::string _client_id;

  // --- Classe que define o contexto para operações MQTT
  class Contexto {
  protected:
    bool _res = false;                      /** @brief Resultado da operação */
    bool _finalizada = false;               /** @brief Define se a operação foi finalizada */
    MQTTClient &_client;
    std::condition_variable _finalizada_cv;
    std::mutex _finalizada_mtx;
    
    void notificar_um();
    bool esperar();
  public:
    explicit Contexto(MQTTClient *client) noexcept : _client(*client) {}
    virtual ~Contexto() noexcept {}
    virtual bool despachar() = 0;
  };

  // === Membros responsáveis pelas (des)conexões
  std::mutex _conexao_mtx;

  // --- Gerência de conexão
  class Connect : public Contexto {
  private:
    static const int _KEEPALIVE_INTERVAL = 30; // Keepalive tcp
    static const int _CLEANSESSION = 0;        // Caso seja `1`, toda nova conexão iniciará com uma sessão limpa.
    static const int _CONNECT_TOUT = 10;       // Timeout de conexão.
    static const int _RETRY_INTERVAL = 3;      // Caso uma mensagem seja enviada e o PUBACK ou PUBREC seja reconhecido.
    MQTTAsync_connectOptions _opts = MQTTAsync_connectOptions_initializer;

    bool _primeira_tentativa = true;

    static void falha(void *ctx, MQTTAsync_failureData *res);
    static void sucesso(void *ctx, MQTTAsync_successData *res);
    static void perdida(void *ctx, char* cause);
    static int mensagemRecebida(void* op, char* topico, int topico_sz, MQTTAsync_message* msg);
  public:
    explicit Connect(MQTTClient *client) noexcept;
    ~Connect() noexcept {}
    bool despachar();
  };
  
  Connect _conexao;

  // === Membros responsáveis pelos (un)subscribes.
  std::unordered_map<std::string, MQTTAsync_messageArrived *> _callback_map;
  SRWLOCK _callback_map_slock = SRWLOCK_INIT;

  // --- Gerência de subscribes
  class Subscribe : public Contexto {
  private:
    const std::vector<const char *> &_topicos;
    const std::vector<MQTTAsync_messageArrived *> &_callbacks;
    const std::vector<int> _qos_requisitados = std::vector<int>(_topicos.size(), 1);

    MQTTAsync_responseOptions _opts = MQTTAsync_responseOptions_initializer;

    static void falha(void *ctx, MQTTAsync_failureData *res);
    static void sucesso(void *ctx, MQTTAsync_successData *res);
  public:
    explicit Subscribe(MQTTClient *client, const std::vector<const char *> &topicos, const std::vector<MQTTAsync_messageArrived *> &callbacks) noexcept;
    ~Subscribe() noexcept {}
    bool despachar();
  };

  // --- Gerência de unsubscribes
  class Unsubscribe : public Contexto {
  private:
    const std::vector<const char *> &_topicos;

    MQTTAsync_responseOptions _opts = MQTTAsync_responseOptions_initializer;

    static void falha(void *ctx, MQTTAsync_failureData *res);
    static void sucesso(void *ctx, MQTTAsync_successData *res);
  public:
    explicit Unsubscribe(MQTTClient *client, const std::vector<const char *> &topicos) noexcept;
    ~Unsubscribe() noexcept {}
    bool despachar();
  };

  // -- Gerência de desconexões
  class Disconnect : public Contexto {
  private:
    const static int _DISCONNECT_TOUT = 10;
    MQTTAsync_disconnectOptions _opts = MQTTAsync_disconnectOptions_initializer;

    static void falha(void *ctx, MQTTAsync_failureData *res);
    static void sucesso(void *ctx, MQTTAsync_successData *res);
  public:
    explicit Disconnect(MQTTClient *client) noexcept;
    ~Disconnect() noexcept {}
    bool despachar();
  };

  Disconnect _desconexao;

public:
  explicit MQTTClient(const std::string &broker_uri, const std::string &client_id) noexcept;
  ~MQTTClient() noexcept;
  
  inline bool conectar() { return _conexao.despachar(); }

  inline bool conectado() const { return _dll.isConnected(_handle); }

  inline bool subscribe(const std::vector<const char *> &topicos, const std::vector<MQTTAsync_messageArrived *> &callbacks) {
    return Subscribe(this, topicos, callbacks).despachar();
  }

  inline bool unsubscribe(const std::vector<const char *> &topicos) {
    return Unsubscribe(this, topicos).despachar();
  }

  inline void desconectar() { _desconexao.despachar(); }
};