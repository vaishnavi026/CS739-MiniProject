#include "client_interface.h"
#include "KeyValueController.grpc.pb.h"
#include "KeyValueController.pb.h"
#include <cstdlib>
#include <fstream>
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <random>
#include <ranges>
#include <stdio.h>
#include <stdlib.h>
#include <string>
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
std::map<std::string, std::unique_ptr<kvstore::KVStore::Stub>> kvstore_map;
std::unique_ptr<kvstore::KVStore::Stub> kvstore_stub = nullptr;
int connection_try_limit = 15;
int restart_try_limit = 15;
int total_servers;
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
  total_servers = servers.size();
  if (servers.empty()) {
    std::cerr << "No valid servers found in config file\n";
    return -1;
  }
  for (const auto &address : servers) {
    auto channel =
        grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    kvstore_map[address] = kvstore::KVStore::NewStub(channel);
    if (!kvstore_map[address]) {
      std::cerr << "Failed to create gRPC stub\n";
    }
    grpc_connectivity_state state = channel->GetState(true);
    if (state == GRPC_CHANNEL_SHUTDOWN ||
        state == GRPC_CHANNEL_TRANSIENT_FAILURE) {
      std::cerr << "Failed to establish gRPC channel connection\n";
    } else {
      num_servers_successful++;
    }
  }
  if (num_servers_successful == 0)
    return -1;
  else
    return 0;
}

int kv739_shutdown(void) {
  for (const auto &address : servers) {
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

int kv739_restart(char *server_name) {

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
  if (state == GRPC_CHANNEL_READY) {
    std::cerr << "Server already started\n" << std::endl;
    return 0;
  } else {
    std::cout
        << "Failed to establish gRPC channel connection which is expected\n";
  }

  // Command to restart
  int retries = 0;
  std::string num_servers = std::to_string(servers.size());
  std::string server_launch_executable = "./kvstore_server";
  pid_t pid = fork();

  if (pid < 0) {
    std::cerr << "Server process relaunch failed\n";
    return -1;
  } else if (pid == 0) {
    FILE *devNull = fopen("/dev/null", "w");
    if (devNull == nullptr) {
      std::cerr << "Failed to open /dev/null: " << strerror(errno) << std::endl;
      exit(EXIT_FAILURE);
    }

    dup2(fileno(devNull), STDOUT_FILENO);
    fclose(devNull);

    char *args[] = {(char *)server_launch_executable.c_str(),
                    (char *)server_address.c_str(), (char *)num_servers.c_str(),
                    nullptr};
    int execvp_status_code = execvp(args[0], args);

    if (execvp_status_code == -1) {
      std::cerr << "Terminated incorrectly\n";
      exit(EXIT_FAILURE);
    }
  } else {
    sleep(1);
    while (state != GRPC_CHANNEL_READY) {
      std::cout << "Failed to establish gRPC channel connection. Retrying"
                << std::endl;
      state = channel->GetState(true);

      if (retries >= restart_try_limit) {
        std::cerr << "Failed to restart even after " << restart_try_limit
                  << " tries" << std::endl;
        return -1;
      }

      sleep(2);

      retries++;
    }

    kvstore_map[server_address] = std::move(temp_stub);
    std::cout << "Restart of server " << server_address << " successful"
              << std::endl;
  }

  return 0;
}

int kv739_get(char *key, char *value) {

  GetRequest request;
  GetReponse response;
  int num_tries;

  std::vector<int> server_ports(connection_try_limit);
  auto gen = std::mt19937{std::random_device{}()};
  std::ranges::sample(std::views::iota(50051, 50051 + total_servers),
                      server_ports.begin(), connection_try_limit, gen);
  std::ranges::shuffle(server_ports, gen);
  std::string server_address;

  if (!is_valid_key(key)) {
    std::cerr << "Key does not meet the conditions set forth" << std::endl;
    return -1;
  }

  request.set_key(key);
  request.set_is_client_request(true);

  num_tries = 0;

  for (int port : server_ports) {
    server_address = "0.0.0.0:" + std::to_string(port);

    if (!kvstore_map[server_address]) {
      std::cerr << "Client not initialized in kv739_get, call kv739_init\n";
      return -1;
    }
    ClientContext context;
    Status status =
        kvstore_map[server_address]->Get(&context, request, &response);
    num_tries++;

    if (!status.ok()) {
      if (num_tries == connection_try_limit) {
        std::cerr << "Server Get Connection retry limit reached, Aborting "
                     "client request"
                  << std::endl;
        return -1;
      }
    } else {
      break;
    }
  }

  int response_code = response.code();
  if (response_code == 0) {
    strcpy(value, response.value().c_str());
    return 0;
  }
  return response_code;
}

int kv739_put(char *key, char *value, char *old_value) {

  PutRequest request;
  PutResponse response;
  int num_tries;

  std::vector<int> server_ports(connection_try_limit);
  auto gen = std::mt19937{std::random_device{}()};
  std::ranges::sample(std::views::iota(50051, 50051 + total_servers),
                      server_ports.begin(), connection_try_limit, gen);
  std::ranges::shuffle(server_ports, gen);
  std::string server_address;

  if (!is_valid_key(key) || !is_valid_value(value)) {
    std::cerr << "Key or Value does not meet the conditions set forth"
              << std::endl;
    return -1;
  }

  request.set_key(key);
  request.set_value(value);
  request.set_is_client_request(true);

  num_tries = 0;

  for (int port : server_ports) {
    server_address = "0.0.0.0:" + std::to_string(port);

    if (!kvstore_map[server_address]) {
      std::cerr << "Client not initialized in kv739_put, call kv739_init\n";
      return -1;
    }

    ClientContext context;
    Status status =
        kvstore_map[server_address]->Put(&context, request, &response);
    num_tries++;

    if (!status.ok()) {
      if (num_tries == connection_try_limit) {
        std::cerr << "Server Put Connection retry limit reached, Aborting "
                     "client request"
                  << std::endl;
        return -1;
      }
    } else {
      break;
    }
  }

  int response_code = response.code();
  if (response_code == 0) {
    strcpy(old_value, response.message().c_str());
  }

  return response_code;
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

int main(int argc, char **argv) { return 0; }