#include "KeyValueController.grpc.pb.h"
#include "KeyValueController.pb.h"
#include "keyValueStore.h"
#include <chrono>
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>

using grpc::Channel;
using grpc::ClientContext;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using kvstore::DieRequest;
using kvstore::Empty;
using kvstore::GetReponse;
using kvstore::GetRequest;
using kvstore::HeartbeatMessage;
using kvstore::KVStore;
using kvstore::PutRequest;
using kvstore::PutResponse;

class KVStoreServiceImpl final : public KVStore::Service {
private:
  std::mutex change_primary;
  keyValueStore kvStore;
  std::string server_address;
  int total_servers;
  bool is_primary;
  std::string primary_address;
  std::map<std::string, std::unique_ptr<KVStore::Stub>> kvstore_stubs_map;
  std::chrono::high_resolution_clock::time_point last_heartbeat;

public:
  KVStoreServiceImpl(std::string &server_address, int total_servers,
                     bool is_primary) {
    this->server_address = server_address;
    this->total_servers = total_servers;
    this->is_primary = is_primary;
    this->primary_address = "0.0.0.0:50051";
    this->last_heartbeat = std::chrono::high_resolution_clock::now();
    kvStore = keyValueStore(server_address);

    if (is_primary) {
      InitializeServerStubs();
    }
  }

  Status Put(ServerContext *context, const PutRequest *request,
             PutResponse *response) override {
    // std::cout << "Received PUT request with key, value \n";
    // std::cout << request->key() << " " << request->value() << std::endl;

    std::string old_value;
    int response_write;
    // response = kvStore.write(request->key().c_str(),
    // request->value().c_str());
    response_write = kvStore.write(request->key(), request->value(),
                                   request->timestamp(), old_value);
    // std::cout << response_write << "\n";

    if (response_write == 0) {
      return grpc::Status(grpc::StatusCode::ALREADY_EXISTS, old_value);
    } else if (response_write == 1) {
      return Status::OK;
    }
    return grpc::Status(grpc::StatusCode::ABORTED, "");
  }

  Status Get(ServerContext *context, const GetRequest *request,
             GetReponse *response) override {
    // std::cout << "Received GET request with key \n";
    // std::cout << request->key() << std::endl;

    std::string value;
    int response_read;
    // if (kvStore.read(request->key().c_str(), value) == 0) {
    //     response->set_value(value);
    // } else {
    //     response->set_value("Key not found");

    response_read = kvStore.read(request->key(), value);

    if (response_read == 0) {
      response->set_value(value);
      return Status::OK;
    } else if (response_read == 1) {
      return grpc::Status(grpc::StatusCode::NOT_FOUND, "");
    }
    return grpc::Status(grpc::StatusCode::ABORTED, "");
  }

  Status Die(ServerContext *context, const DieRequest *request,
             Empty *response) override {
    int clean_code = request->clean();
    if (clean_code == 1) {
      // TODO: If primary, complete state replciation and new election.
      std::cout << "State flushed, server shutting down";
    } else {
      std::cout << "Server killed";
    }
    exit(1);
  }

  Status Heartbeat(ServerContext *context, const HeartbeatMessage *request,
                   Empty *response) override {
    std::cout << "Received Heartbeat" << std::endl;
    std::unique_lock<std::mutex> primary_lock(change_primary);
    this->last_heartbeat = std::chrono::high_resolution_clock::now();
    this->primary_address = request->primary();
    return Status::OK;
  }

  void HeartbeatMechanism() {
    while (true) {
      if (is_primary) {
        SendHeartbeats();
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
      } else {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        CheckLastHeartbeat();
      }
    }
  }

  void CheckLastHeartbeat() {
    std::chrono::high_resolution_clock::time_point current_time =
        std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> duration_milli =
        std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(
            current_time - last_heartbeat);
    std::cout << "Last heartbeat " << duration_milli.count() << std::endl;
    // if (duration_milli.count() > 300) {
    //   std::unique_lock<std::mutex> primary_lock(change_primary);
    //   this->primary_address = server_address;
    //   this->is_primary = true;
    //   InitializeServerStubs();
    //   SendHeartbeats();
    // }
  }

  void SendHeartbeats() {
    for (const auto &pair : kvstore_stubs_map) {
      std::cout << pair.first << std::endl;
      HeartbeatMessage message;
      ClientContext context;
      Empty response;

      message.set_primary(server_address);
      pair.second->Heartbeat(&context, message, &response);
    }
  }

  void InitializeServerStubs() {
    for (int port = 50051; port < 50051 + total_servers; port++) {
      std::string address("0.0.0.0:" + std::to_string(port));
      auto channel =
          grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
      kvstore_stubs_map[address] = kvstore::KVStore::NewStub(channel);
      if (!kvstore_stubs_map[address]) {
        std::cerr << "Failed to create gRPC stub\n";
      }
      grpc_connectivity_state state = channel->GetState(true);
      if (state == GRPC_CHANNEL_SHUTDOWN ||
          state == GRPC_CHANNEL_TRANSIENT_FAILURE) {
        std::cerr << "Failed to establish gRPC channel connection\n";
      }
    }
  }
};

void RunServer(std::string &server_address, int total_servers) {
  int port = std::stoi(server_address.substr(server_address.find(":") + 1,
                                             server_address.size()));
  bool is_primary = port == 50051;
  KVStoreServiceImpl service(server_address, total_servers, is_primary);

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());

  if (!server) {
    std::cerr << "Failed to start server on " << server_address << std::endl;
    exit(1);
  }

  std::thread t1(&KVStoreServiceImpl::HeartbeatMechanism, &service);

  std::cout << "Server started at " << server_address << std::endl;
  server->Wait();

  t1.join();
}

int main(int argc, char **argv) {
  std::string server_address("0.0.0.0:50051");
  int total_servers = 10;
  if (argc > 1) {
    server_address = argv[1];
    total_servers = std::atoi(argv[2]);
  }

  RunServer(server_address, total_servers);
  return 0;
}
