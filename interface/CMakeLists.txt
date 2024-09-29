get_filename_component(KeyValueController_proto "${CMAKE_CURRENT_SOURCE_DIR}/KeyValueController.proto" ABSOLUTE)
get_filename_component(KeyValueController_proto_path "${KeyValueController_proto}" PATH)

set(KeyValueController_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/KeyValueController.pb.cc")
set(KeyValueController_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/KeyValueController.pb.h")
set(KeyValueController_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/KeyValueController.grpc.pb.cc")
set(KeyValueController_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/KeyValueController.grpc.pb.h")

add_custom_command(
      OUTPUT "${KeyValueController_proto_srcs}" "${KeyValueController_proto_hdrs}" "${KeyValueController_grpc_srcs}" "${KeyValueController_grpc_hdrs}"
      COMMAND ${_PROTOBUF_PROTOC}
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${KeyValueController_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${KeyValueController_proto}"
      DEPENDS "${KeyValueController_proto}")

include_directories("${CMAKE_CURRENT_BINARY_DIR}")

add_library(KeyValueController_proto_grpc
  ${KeyValueController_proto_srcs}
  ${KeyValueController_proto_hdrs}
  ${KeyValueController_grpc_srcs}
  ${KeyValueController_grpc_hdrs})

target_link_libraries(KeyValueController_proto_grpc
  absl::check
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})
