#include "KeyValueController.grpc.pb.h"
#include "KeyValueController.pb.h"
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <string>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using kvstore::GetReponse;
using kvstore::GetRequest;
using kvstore::KVStore;
using kvstore::PutRequest;
using kvstore::PutResponse;

class KVStoreServiceImpl final : public KVStore::Service {
public:
  Status Put(ServerContext *context, const PutRequest *request,
             PutResponse *response) override {
    std::cout << "Received PUT request with key, value \n";
    std::cout << request->key() << " " << request->value() << std::endl;

    response->set_message("HELLO FROM SERVER PUT");
    return Status::OK;
  }

  Status Get(ServerContext *context, const GetRequest *request,
             GetReponse *response) override {
    std::cout << "Received GET request with key \n";
    std::cout << request->key() << std::endl;

    response->set_value("HELLO FROM SERVER GET");
    return Status::OK;
  }
};

void RunServer() {
  std::string server_address("0.0.0.0:50051");
  KVStoreServiceImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());

  server->Wait();
}

int main(int argc, char **argv) {
  RunServer();
  return 0;
}
