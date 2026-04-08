cmake_minimum_required(VERSION 3.31)
project(MQTT-test)

string(ASCII 27 Esc)
set(Verde "${Esc}[32m")
set(VerdeNegr "${Esc}[1;32m")
set(Branco "${Esc}[m")
set(VermNegr "${Esc}[1;31m")
set(AmareloNegr "${Esc}[1;33m")
set(CianoNegr "${Esc}[1;36m")

# Função para logging e debugging.
function(message)
  list(GET ARGV 0 MessageType)

  if(MessageType STREQUAL FATAL_ERROR OR MessageType STREQUAL SEND_ERROR)
    list(REMOVE_AT ARGV 0)
    _message(${MessageType} "${VermNegr}${ARGV}${Branco}")
  elseif(MessageType STREQUAL WARNING)
    list(REMOVE_AT ARGV 0)
    _message(STATUS "${AmareloNegr}${ARGV}${Branco}")
  elseif(MessageType STREQUAL AUTHOR_WARNING)
    list(REMOVE_AT ARGV 0)
    _message(STATUS "${CianoNegr}${ARGV}${Branco}")
  elseif(MessageType STREQUAL STATUS)
    list(REMOVE_AT ARGV 0)
    _message(STATUS "${Verde}${ARGV}${Branco}")
  elseif(MessageType STREQUAL STATUS_IMPORTANT)
    list(REMOVE_AT ARGV 0)
    _message(STATUS "${VerdeNegr}${ARGV}${Branco}")
  elseif(MessageType STREQUAL CONFIGURE_LOG)
  elseif(MessageType STREQUAL CHECK_START)
  elseif(MessageType STREQUAL CHECK_PASS)
  else()
    _message("${ARGV}")
  endif()
endfunction()

# Usamos o padrão do C++20.
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_SCAN_FOR_MODULES OFF)
unset(CMAKE_CXX_VERBOSE_FLAG)
unset(CMAKE_C_VERBOSE_FLAG)


set(CMAKE_CXX_FLAGS_RELEASE "-Wall -Wextra -Werror -O3 -march=i686 -mtune=generic")
# Caso esteja compilando no modo 'Debug', as libs do compilador serão linkadas dinamicamente. Neste caso, cuidado
# com o caminho das libs do compilador. Se elas estiverem em um caminho que o executável não tenha permissão para abrir,
# o serviço não será executado (por exemplo, no Windows, o serviço roda na conta LocalSystem que não tem permissão para
# acessar alguns diretórios de usuário).
set(CMAKE_CXX_FLAGS_DEBUG "-Wall -Wextra -O0 -g -DMQTT_TEST_DBG")

# Restringir ao compilador GCC por hora.
if(NOT "${CMAKE_CXX_COMPILER_ID}" MATCHES "GNU")
  message(FATAL_ERROR "Este projeto aceita apenas o GNU C Compiler como compilador.")
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "Windows")
  # Flags globais exclusivas do Windows
  # -D_WIN32_WINNT=0x0A00: Indica à API do Windows que queremos compatibilidade com, no mínimo, Windows 10.
  # -DWIN32_LEAN_AND_MEAN: Requerimento da lib cpp-httplib. Veja o README da mesma para mais detalhes.
  # -static:               é a responsável por linkar as libs do compilador estaticamente ao executável.
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_WIN32_WINNT=0x0A00 -DWIN32_LEAN_AND_MEAN")
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -static")
  
else()
  message(FATAL_ERROR "Este programa não pode ser compilado para este sistema operacional.")
endif()

message(STATUS_IMPORTANT "CMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}")
message(STATUS_IMPORTANT "OS_SPECIFIC_LIBS=${OS_SPECIFIC_LIBS}")

# Configurações baseadas no tipo de build
if("${CMAKE_BUILD_TYPE}" STREQUAL "Release" OR "${CMAKE_BUILD_TYPE}" STREQUAL "")
  message(STATUS_IMPORTANT "CMAKE_CXX_FLAGS_RELEASE=${CMAKE_CXX_FLAGS_RELEASE}")
  message(STATUS "O projeto está sendo configurado para 'Release'.")
elseif("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
  message(STATUS_IMPORTANT "CMAKE_CXX_FLAGS_DEBUG=${CMAKE_CXX_FLAGS_DEBUG}")
  message(STATUS "O projeto está sendo configurado para 'Debug'.")
else()
  message(FATAL_ERROR "O CMAKE_BUILD_TYPE escolhido não é suportado.")
endif()

set(SRCDIR "${CMAKE_CURRENT_SOURCE_DIR}/src")
add_subdirectory(src)