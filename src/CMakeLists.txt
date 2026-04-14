add_executable(mqtt-test mqtt-test.cpp)

add_subdirectory(MQTT)

target_link_libraries(mqtt-test PRIVATE mqtt)
target_include_directories(mqtt-test PRIVATE "${SRCDIR}")

install(TARGETS mqtt-test DESTINATION "install")