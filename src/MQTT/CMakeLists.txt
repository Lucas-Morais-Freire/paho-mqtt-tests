add_library(mqtt STATIC
  MQTTClient.cpp
)

add_subdirectory(MQTTDll)

target_link_libraries(mqtt PRIVATE mqttdll)
target_include_directories(mqtt PRIVATE "${SRCDIR}")