#include "KeyValueController.grpc.pb.h"
#include "KeyValueController.pb.h"
#include "keyValueStore.h"
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <string>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using kvstore::DieRequest;
using kvstore::Empty;
using kvstore::GetReponse;
using kvstore::GetRequest;
using kvstore::KVStore;
using kvstore::PutRequest;
using kvstore::PutResponse;

class KVStoreServiceImpl final : public KVStore::Service {
private:
  keyValueStore kvStore;
  int total_servers;

public:
  KVStoreServiceImpl(int server_count) { total_servers = server_count; }

  Status Put(ServerContext *context, const PutRequest *request,
             PutResponse *response) override {
    // std::cout << "Received PUT request with key, value \n";
    // std::cout << request->key() << " " << request->value() << std::endl;

    std::string old_value;
    int response_write;
    // response = kvStore.write(request->key().c_str(),
    // request->value().c_str());
    response_write = kvStore.write(request->key(), request->value(), old_value);
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
};

void RunServer(std::string &server_address, int server_count) {
  KVStoreServiceImpl service(server_count);

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());

  if (!server) {
    std::cerr << "Failed to start server on " << server_address << std::endl;
    exit(1);
  }
  std::cout << "Server started at " << server_address << std::endl;
  server->Wait();
}

int main(int argc, char **argv) {
  std::string server_address("0.0.0.0:50051");
  int server_count = 10;
  if (argc > 1) {
    server_address = argv[1];
    server_count = std::atoi(argv[2]);
  }

  RunServer(server_address, server_count);
  return 0;
}
