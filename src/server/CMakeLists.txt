include_directories(${CMAKE_CURRENT_BINARY_DIR}/../../interface)

find_package(SQLite3 REQUIRED)
include_directories(${SQLite3_INCLUDE_DIRS})

add_library(keyValueStoreLib keyValueStore.cpp)

add_executable(server server.cpp)
    target_link_libraries(server
	keyValueStoreLib
        KeyValueController_proto
        KeyValueController_grpc
        ${SQLite3_LIBRARIES}
        ${_PROTOBUF_LIBPROTOBUF}
        ${_REFLECTION}
        ${_GRPC_GRPCPP})



