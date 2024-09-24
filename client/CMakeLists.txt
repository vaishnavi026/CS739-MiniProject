include_directories(${CMAKE_CURRENT_BINARY_DIR}/../interface)
set(CMAKE_SHARED_LIBRARY_SUFFIX ".so")
add_library(kv739 SHARED client_interface.cpp)
    target_link_libraries(kv739
        KeyValueController_proto
        KeyValueController_grpc
        ${_PROTOBUF_LIBPROTOBUF}
        ${_REFLECTION}
        ${_GRPC_GRPCPP})