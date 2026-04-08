add_executable(mqtt-test mqtt-test.cpp)

add_subdirectory(MQTTDll)

target_link_libraries(mqtt-test PRIVATE mqttdll)
target_include_directories(mqtt-test PRIVATE "${SRCDIR}")

install(TARGETS mqtt-test DESTINATION bin)