add_library(mqttdll STATIC
  MQTTDll.cpp
)

target_include_directories(mqtt-test PRIVATE "${SRCDIR}")

install(FILES "libpaho-mqtt3a.dll" DESTINATION "install")