#pragma once

#include "MQTTDll/MQTTDll.h"
#include <condition_variable>
#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <windows.h>

class MQTTClient {
private:
  // --- lib
  static std::mutex _dll_mtx;
  static MQTTDll _dll;

  // --- Classe que define o contexto para operações MQTT
  class Operacao {
  protected:
    bool _res = false;                      /** @brief Resultado da operação */
    bool _finalizada = false;               /** @brief Define se a operação foi finalizada */
    MQTTClient &_client;
    std::condition_variable _finalizada_cv;
    std::mutex _finalizada_mtx;
    explicit Operacao(MQTTClient &client) noexcept : _client(client) {}

  public:
    inline bool resultado() { return _res; };
    void notificar_um();
    void esperar();
  };

  // --- Operação de conexão
  class Conexao : public Operacao {
  public:
    static void falha(void *op, MQTTAsync_failureData *res);
    static void sucesso(void *op, MQTTAsync_successData *res);
    explicit Conexao(MQTTClient &client) noexcept : Operacao(client) {}
    inline MQTTClient &getClient() { return _client; }
  };
  
  // --- Dados da conexão
  static const int _KEEPALIVE_INTERVAL = 30; // Keepalive tcp
  static const int _CLEANSESSION = 1;        // Caso seja `1`, toda nova conexão iniciará com uma sessão limpa.
  static const int _CONNECT_TOUT = 10;       // Timeout de conexão.
  static const int _RETRY_INTERVAL = 3;      // Caso uma mensagem seja enviada e o PUBACK ou PUBREC seja reconhecido.
  MQTTAsync _client = NULL;
  std::string _broker_uri;
  std::string _client_id;
  Conexao _conexao;
  std::mutex _conexao_mtx;
  bool _primeira_conexao = true;
  static void conexaoPerdida(void* op, char* cause);
  
  // --- Operação de inscrição
  std::unordered_map<std::string, MQTTAsync_messageArrived *> _callback_map;
  SRWLOCK _callback_map_slock = SRWLOCK_INIT;
  static int mensagemRecebida(void* op, char* topico, int topico_sz, MQTTAsync_message* msg);

  class Subscribe : public Operacao {
  private:
    const std::vector<int>    &_qos_requisitados;
    const std::vector<char *> &_topicos;

  public:
    inline explicit Subscribe(MQTTClient &client, const std::vector<int> &qosRequeridos, const std::vector<char *> &topicos) noexcept :
    Operacao(client), _qos_requisitados(qosRequeridos), _topicos(topicos) {}
    static void falha(void *op, MQTTAsync_failureData *res);
    static void sucesso(void *op, MQTTAsync_successData *res);
  };

  // --- Operação de unsubscribe
  class Unsubscribe : public Operacao {
  private:
    const std::vector<char *> &_topicos;

  public:
    inline explicit Unsubscribe(MQTTClient &client, const std::vector<char *> &topicos) noexcept : Operacao(client), _topicos(topicos) {}
    static void falha(void *op, MQTTAsync_failureData *res);
    static void sucesso(void *op, MQTTAsync_successData *res);
  };

public:
  explicit MQTTClient(const std::string &broker_uri, const std::string &client_id);
  
  bool conectar();

  inline bool conectado() { return _dll.isConnected(_client); }

  bool subscribe(const std::vector<std::string> &topicos, const std::vector<MQTTAsync_messageArrived *> &callbacks);

  bool unsubscribe(const std::vector<std::string> &topicos);
};