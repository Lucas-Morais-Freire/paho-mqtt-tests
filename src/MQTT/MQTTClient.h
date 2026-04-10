#pragma once

#include "MQTTDll/MQTTDll.h"
#include <condition_variable>
#include <string>
#include <vector>
#include <memory>

class MQTTClient {
private:
  // --- lib
  inline static std::mutex _dll_mtx;
  static MQTTDll _dll;

  // --- Classe que define o contexto para operações MQTT
  class Operacao {
  protected:
    bool _res = false;                      /** @brief Resultado da operação */
    bool _finalizada = false;               /** @brief Define se a operação foi finalizada */
    const std::string &_broker_uri;
    std::condition_variable _finalizada_cv;
    std::mutex _finalizada_mtx;
    explicit Operacao(const std::string &broker_uri) noexcept : _broker_uri(broker_uri) {}

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
    explicit Conexao(const std::string &broker_uri) noexcept : Operacao(broker_uri) {}
  };
  
  // --- Dados da conexão
  static const int _KEEPALIVE_INTERVAL = 30; // Keepalive tcp
  static const int _CLEANSESSION = 1;        // Caso seja `1`, toda nova conexão iniciará com uma sessão limpa.
  static const int _CONNECT_TOUT = 10;       // Timeout de conexão.
  static const int _RETRY_INTERVAL = 3;      // Caso uma mensagem seja enviada e o PUBACK ou PUBREC seja reconhecido.
  MQTTAsync _client;
  std::string _broker_uri;
  std::string _client_id;
  Conexao _conexao;
  std::mutex _conexao_mtx;
  static void conexaoPerdida(void* client, char* cause);
  static int mensagemRecebidaIgnorar(void* op, char* topico, int topico_sz, MQTTAsync_message* msg);

  // --- Operação de inscrição
  struct Subscribe : public Operacao {
  private:
    const std::vector<int>    &_qosRequisitados;
    const std::vector<char *> &_topicos;

  public:
    explicit Subscribe(const std::string &broker_uri, const std::vector<int> &qosRequeridos, const std::vector<char *> &topicos) noexcept :
    Operacao(broker_uri), _qosRequisitados(qosRequeridos), _topicos(topicos) {}
    static void falha(void *op, MQTTAsync_failureData *res);
    static void sucesso(void *op, MQTTAsync_successData *res);
  };

  // Callbacks para mensagens recebidas.
  static int mensagemRecebidaCallback(void* op, char* topico, int topico_sz, MQTTAsync_message* msg);

public:
  explicit MQTTClient(const std::string &broker_uri, const std::string &client_id);
  
  bool conectar();

  inline bool conectado() { return _dll.isConnected(_client); }

  bool subscribe(const std::vector<char *> &topicos);
  inline bool subscribe(const std::string &topico) {
    std::vector<char *> topicos(1, const_cast<char *>(topico.data()));
    return subscribe(topicos);
  }
};