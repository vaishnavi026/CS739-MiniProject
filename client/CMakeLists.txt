include_directories(${CMAKE_CURRENT_BINARY_DIR}/../interface)

add_executable(client_interface client_interface.cpp)
    target_link_libraries(client_interface
        KeyValueController_proto
        KeyValueController_grpc
        ${_PROTOBUF_LIBPROTOBUF}
        ${_REFLECTION}
        ${_GRPC_GRPCPP})