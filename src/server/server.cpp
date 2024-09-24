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
private:
  KeyValueStore kvStore;
public:
  Status Put(ServerContext *context, const PutRequest *request,
             PutResponse *response) override {
    std::cout << "Received PUT request with key, value \n";
    std::cout << request->key() << " " << request->value() << std::endl;

    // int response = kvStore.put(request->key().c_str(), request->value().c_str());

    response->set_message("HELLO FROM SERVER PUT");
    return Status::OK;
  }

  Status Get(ServerContext *context, const GetRequest *request,
             GetReponse *response) override {
    std::cout << "Received GET request with key \n";
    std::cout << request->key() << std::endl;
      
    // char value[256];  
    // if (kvStore.get(request->key().c_str(), value) == 0) {
    //     response->set_value(value);
    // } else {
    //     response->set_value("Key not found");  

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
