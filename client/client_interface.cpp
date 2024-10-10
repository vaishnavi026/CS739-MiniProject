#include "client_interface.h"
#include "KeyValueController.grpc.pb.h"
#include "KeyValueController.pb.h"
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <fstream>

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using kvstore::DieRequest;
using kvstore::Empty;
using kvstore::GetReponse;
using kvstore::GetRequest;
using kvstore::KVStore;
using kvstore::PutRequest;
using kvstore::PutResponse;

std::vector<std::string> servers;
std::map<std::string,std::unique_ptr<kvstore::KVStore::Stub>> kvstore_map;
std::unique_ptr<kvstore::KVStore::Stub> kvstore_stub = nullptr;

bool is_valid_value(char *value);
bool is_valid_key(char *key);

int kv739_init(char *config_file) {

  std::ifstream file(config_file);
  if (!file.is_open()) {
    std::cerr << "Unable to open config file: " << config_file << std::endl;
    return -1;
  }

  std::string server_address;
  int num_servers_successful = 0;
  while (std::getline(file, server_address)) {
    if (!server_address.empty()) {
      servers.push_back(server_address);
      kvstore_map[server_address] = nullptr;
    }
  }

  file.close();

  if (servers.empty()) {
    std::cerr << "No valid servers found in config file\n";
    return -1;
  }

  for (const auto& address : servers) {
    auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    kvstore_map[address] = kvstore::KVStore::NewStub(channel);
    if (!kvstore_map[address]) {
      std::cerr << "Failed to create gRPC stub\n";
    }
    grpc_connectivity_state state = channel->GetState(true);
    if (state == GRPC_CHANNEL_SHUTDOWN || state == GRPC_CHANNEL_TRANSIENT_FAILURE) {
      std::cerr << "Failed to establish gRPC channel connection\n";
    }else{
      num_servers_successful++;
    }
  }

  if(num_servers_successful == 0)
      return -1;
  else
      return 0;
}

int kv739_shutdown(void) {

  for (const auto& address : servers) {
      kvstore_map[address].reset();
  }

  servers.clear();
  
  return 0;
}

int kv739_die(char *server_name, int clean) {
  std::string server_address(server_name);
  auto channel =
      grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
  std::unique_ptr<kvstore::KVStore::Stub> temp_stub =
      kvstore::KVStore::NewStub(channel);
  if (!temp_stub) {
    std::cerr << "Failed to create gRPC stub\n";
    return -1;
  }
  grpc_connectivity_state state = channel->GetState(true);
  if (state == GRPC_CHANNEL_SHUTDOWN ||
      state == GRPC_CHANNEL_TRANSIENT_FAILURE) {
    std::cerr << "Failed to establish gRPC channel connection\n";
    return -1;
  }
  DieRequest request;
  ClientContext context;
  Empty response;

  request.set_clean(clean);
  temp_stub->Die(&context, request, &response);

  return 0;
}

int kv739_get(char *key, char *value) {
  if (!kvstore_stub) {
    std::cerr << "Client not initialized, call kv739_init\n";
    return -1;
  }

  GetRequest request;

  if (!is_valid_key(key)) {
    std::cerr << "Key does not meet the conditions set forth" << std::endl;
    return -1;
  }

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

  if (is_valid_key(key) && is_valid_value(value)) {
    request.set_key(key);
    request.set_value(value);

    PutResponse response;
    ClientContext context;

    Status status = kvstore_stub->Put(&context, request, &response);

    if (status.error_code() == grpc::StatusCode::ALREADY_EXISTS) {
      strcpy(old_value, status.error_message().c_str());
      return 0;
    } else if (status.ok()) {
      return 1;
    } else {
      std::cerr << "Server Put failed: " << status.error_message() << "\n";
      return -1;
    }
  } else {
    std::cerr << "Key or Value does not meet the conditions set forth"
              << std::endl;
    return -1;
  }
}

bool is_valid_key(char *key) {

  int len = strlen(key);

  if (len == 0 || len > 128) {
    return false;
  }

  for (int i = 0; i < len; ++i) {
    char ch = key[i];
    if (isalnum(ch)) {
      // Allowed case
    } else {
      return false;
    }
  }

  return true;
}

bool is_valid_value(char *value) {

  int len = strlen(value);

  if (len == 0 || len > 2048) {
    return false;
  }

  for (int i = 0; i < len; ++i) {
    char ch = value[i];
    if (isalnum(ch)) {
      // Allowed case
    } else {
      return false;
    }
  }

  return true;
}

int main(int argc, char **argv) {
  char server_name[] = "0.0.0.0:50051";
  if (kv739_init(server_name) != 0) {
    return -1;
  }

  // Expected outputs: With Fresh DB
  // Key Not Found!
  // Put Success, no old value
  // Put Success, old value Distributed Systems, MIKE SWIFT
  // Key Found, Value is Distributed Systems, MIKE SWIFT
  // Put Success, old value Distributed Systems, MIKE SWIFT
  // Key Found, Value is Distributed Systems, MIKE SWIFT, FALL 2024

  char key[] = "CS739";
  char value[] = "Distributed Systems, MIKE SWIFT";
  char update_value[] = "Distributed Systems, MIKE SWIFT, FALL 2024";

  char get_value[2049] = {0};
  int get_result = kv739_get(key, get_value);
  if (get_result == 0) {
    std::cout << "Key Found, Value is " << get_value << "\n";
  } else if (get_result == 1) {
    std::cout << "Key Not Found!\n";
  }

  char old_value[2049] = {0};
  int put_result = kv739_put(key, value, old_value);
  if (put_result == 1) {
    std::cout << "Put Success, no old value\n";
  } else if (put_result == 0) {
    std::cout << "Put Success, old value " << old_value << "\n";
  }

  int put_result2 = kv739_put(key, value, old_value);
  if (put_result2 == 1) {
    std::cout << "Put Success, no old value\n";
  } else if (put_result2 == 0) {
    std::cout << "Put Success, old value " << old_value << "\n";
  }

  int get_result2 = kv739_get(key, get_value);
  if (get_result2 == 0) {
    std::cout << "Key Found, Value is " << get_value << "\n";
  } else if (get_result2 == 1) {
    std::cout << "Key Not Found!\n";
  }

  char old_value2[2049] = {0};
  int put_result3 = kv739_put(key, update_value, old_value2);
  if (put_result3 == 1) {
    std::cout << "Put Success, no old value\n";
  } else if (put_result3 == 0) {
    std::cout << "Put Success, old value " << old_value2 << "\n";
  }

  char get_value2[2049] = {0};
  int get_result3 = kv739_get(key, get_value2);
  if (get_result3 == 0) {
    std::cout << "Key Found, Value is " << get_value2 << "\n";
  } else if (get_result3 == 1) {
    std::cout << "Key Not Found!\n";
  }

  if (kv739_shutdown() != 0) {
    return -1;
  }

  return 0;
}