add_executable(client_interface client_interface.cpp)
    target_link_libraries(client_interface
        KeyValueController_proto
        ${_PROTOBUF_LIBPROTOBUF}
        ${_REFLECTION}
        ${_GRPC_GRPCPP})