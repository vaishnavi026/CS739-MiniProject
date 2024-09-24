#include <iostream>
#include <string>
#include "interface/KeyValueController.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using KVStore::KVStore;
using KVStore::PutRequest;
using KVStore::PutResponse;
using KVStore::GetRequest;
using KVStore::GetReponse;

class KVStoreServiceImpl final : public KVStore::Service {
public:
    Status Put(ServerContext* context, const PutRequest* request, PutResponse* response) override {
        return Status::OK;
    }

    Status Get(ServerContext* context, const GetRequest* request, GetReponse* response) override {
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

int main(int argc, char** argv) {
    RunServer();
    return 0;
}
