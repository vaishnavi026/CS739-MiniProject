include_directories(${CMAKE_CURRENT_BINARY_DIR}/../../interface)

add_executable(server server.cpp)
    target_link_libraries(server
        KeyValueController_proto
        KeyValueController_grpc
        ${_PROTOBUF_LIBPROTOBUF}
        ${_REFLECTION}
        ${_GRPC_GRPCPP})

add_executable(keyValueStore keyValueStore.cpp)
    target_link_libraries(keyValueStore
        KeyValueController_proto
        KeyValueController_grpc
        ${_PROTOBUF_LIBPROTOBUF}
        ${_REFLECTION}
        ${_GRPC_GRPCPP})

add_executable(sqlite_interface sqlite_interface.cpp)
    target_link_libraries(sqlite_interface
        KeyValueController_proto
        KeyValueController_grpc
        ${_PROTOBUF_LIBPROTOBUF}
        ${_REFLECTION}
        ${_GRPC_GRPCPP})


