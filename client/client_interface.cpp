#include "client_interface.h"
#include "KeyValueController.grpc.pb.h"
#include "KeyValueController.pb.h"
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string>

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using kvstore::GetReponse;
using kvstore::GetRequest;
using kvstore::KVStore;
using kvstore::PutRequest;
using kvstore::PutResponse;

std::unique_ptr<kvstore::KVStore::Stub> kvstore_stub = nullptr;

int kv739_init(char *server_name) {
  std::string server_address(server_name);
  auto channel =
      grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
  kvstore_stub = kvstore::KVStore::NewStub(channel);
  if (!kvstore_stub) {
    std::cerr << "Failed to create gRPC stub\n";
    return -1;
  }
  grpc_connectivity_state state = channel->GetState(true);
  if (state == GRPC_CHANNEL_SHUTDOWN ||
      state == GRPC_CHANNEL_TRANSIENT_FAILURE) {
    std::cerr << "Failed to establish gRPC channel connection\n";
    return -1;
  }
  return 0;
}

int kv739_shutdown(void) {
  kvstore_stub.reset();
  return 0;
}

int kv739_get(char *key, char *value) {
  if (!kvstore_stub) {
    std::cerr << "Client not initialized, call kv739_init\n";
    return -1;
  }

  GetRequest request;
  request.set_key(key);

  GetReponse response;
  ClientContext context;

  Status status = kvstore_stub->Get(&context, request, &response);

  if (status.ok()) {
    strcpy(value, response.value().c_str());
    return 0;
  } else if (status.error_code() == grpc::StatusCode::NOT_FOUND) {
    return 1;
  } else {
    std::cerr << "Server Get failed: " << status.error_message() << "\n";
    return -1;
  }
}

int kv739_put(char *key, char *value, char *old_value) {
  if (!kvstore_stub) {
    std::cerr << "Client not initialized, call kv739_init\n";
    return -1;
  }

  PutRequest request;
  request.set_key(key);
  request.set_value(value);

  PutResponse response;
  ClientContext context;

  Status status = kvstore_stub->Put(&context, request, &response);

  if (status.ok()) {
    strcpy(old_value, response.message().c_str());
    return 0;
  } else {
    std::cerr << "Server Put failed: " << status.error_message() << "\n";
    return -1;
  }
}

int main(int argc, char **argv) {
  char server_name[] = "0.0.0.0:50051";
  if (kv739_init(server_name) != 0) {
    return -1;
  }

  char key[] = "CS739";
  char value[] = "Distributed Systems, MIKE SWIFT";
  char old_value[2049] = {0};

  int put_result = kv739_put(key, value, old_value);
  if (put_result == 0) {
    std::cout << "Put response " << old_value << "\n";
  }

  char get_value[2049] = {0};
  int get_result = kv739_get(key, get_value);
  if (get_result == 0) {
    std::cout << "Get response " << get_value << "\n";
  }

  char key2[] = "RANDOM";
  char get_value2[2049] = {0};
  get_result = kv739_get(key2, get_value2);
  if (get_result == 0) {
    std::cout << "Get response " << get_value2 << "\n";
  } else if (get_result == 1) {
    std::cout << "Key Not Found!\n";
  }

  if (kv739_shutdown() != 0) {
    return -1;
  }

  return 0;
}