#pragma once

#ifdef _WIN32

#include <windows.h>

// Safadeza para que a saída do console seja UTF-8
#define main(argc_decl, argv_decl)       \
mqtt_test_main(argc_decl, argv_decl);    \
int main(argc_decl, argv_decl) {         \
  SetConsoleOutputCP(65001);             \
  SetConsoleCP(65001);                   \
  return mqtt_test_main(argc, argv);     \
}                                        \
int mqtt_test_main(argc_decl, argv_decl)

#endif