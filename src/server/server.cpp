#include "KeyValueController.grpc.pb.h"
#include "KeyValueController.pb.h"
#include "keyValueStore.h"
#include <chrono>
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

using grpc::Channel;
using grpc::ClientContext;
using grpc::CompletionQueue;
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
using kvstore::ReplicateRequest;
using kvstore::RestoreServerRequest;
using kvstore::RestoreServerResponse;
using kvstore::KeyValuePair;

int getPortNumber(const std::string &address) {
  size_t colon_pos = address.find(':');
  if (colon_pos == std::string::npos) {
    throw std::invalid_argument("Invalid address format.");
  }

  return std::stoi(address.substr(colon_pos + 1));
}

void AsyncReplicationHelper(const ReplicateRequest &request,
                            const std::unique_ptr<KVStore::Stub> &stub) {
  return;
  ClientContext *context = new ClientContext;
  Empty *response = new Empty;
  CompletionQueue *cq = new CompletionQueue;
  Status status_;

  std::cout << "Received ASYNC REPLICATE request with key \n";
  std::cout << request.key() << std::endl;
  std::cout << "with async forward: " << request.async_forward_to_all()
            << std::endl;

  std::unique_ptr<grpc::ClientAsyncResponseReader<Empty>> rpc(
      stub->AsyncReplicate(context, request, cq));

  rpc->Finish(response, &status_, (void *)1);

  // Use another thread to poll the CompletionQueue for the result
  std::thread([cq, response, context]() {
    std::cout << "Finished ASYNC REPLICATE request. \n";

    void *got_tag;
    bool ok = false;

    // Wait for the result
    cq->Next(&got_tag, &ok);
    // GPR_ASSERT(ok);

    if (ok) {
      std::cout << "Replication completed." << std::endl;
    } else {
      std::cerr << "Replication failed." << std::endl;
    }

    delete response;
    delete context;
    delete cq;
  }).detach();
}

class KVStoreServiceImpl final : public KVStore::Service {
private:
  keyValueStore kvStore;
  ConsistentHashing CH;
  std::string server_address;
  int total_servers;
  int virtual_servers_for_ch;
  int first_port;
  int last_port;
  int replication_factor;
  int write_quorum;
  //   std::atomic_int writes_completed;
  std::map<std::string, std::unique_ptr<KVStore::Stub>> kvstore_stubs_map;
  std::chrono::high_resolution_clock::time_point last_heartbeat;

public:
  KVStoreServiceImpl(std::string &server_address, int total_servers,
                     int virtual_servers_for_ch, int replication_factor)
      : kvStore(server_address), CH(virtual_servers_for_ch) {
    this->server_address = server_address;
    this->total_servers = total_servers;
    this->virtual_servers_for_ch = virtual_servers_for_ch;
    this->first_port = 50051;
    this->last_port = first_port + total_servers - 1;
    this->replication_factor = replication_factor;
    this->write_quorum = (replication_factor + 1) / 2;
    InitializeServerStubs();
  }

  Status Put(ServerContext *context, const PutRequest *request,
             PutResponse *response) override {
    std::string key = request->key();
    std::string value = request->value();
    uint64_t timestamp = request->timestamp();

    if (!request->is_client_request()) {
      std::string old_timestamp_and_value;
      int response_write =
          kvStore.write(key, value, timestamp, old_timestamp_and_value);

      if (response_write == -1) {
        return grpc::Status(grpc::StatusCode::ABORTED, "");
      }

      response->set_code(response_write);

      if (response_write == 0) {
        std::string old_value;
        uint64_t timestamp;
        bool parseSuccessful =
            kvStore.parseValue(old_timestamp_and_value, timestamp, old_value);
        response->set_message(old_value);
        response->set_timestamp(timestamp);
      }
      return Status::OK;
    }

    std::cout << "Received client put key, value = " << key << " " << value
              << std::endl;
    std::string hashed_server = CH.getServer(key);
    int server_port = getPortNumber(hashed_server);

    auto now = std::chrono::system_clock::now();
    timestamp = std::chrono::duration_cast<std::chrono::nanoseconds>(
                    now.time_since_epoch())
                    .count();

    std::cout << "Setting client put timestamp = " << timestamp << std::endl;
    std::mutex put_response_mutex;
    std::atomic<int> last_server_port_tried = (server_port + write_quorum - 1);
    if (last_server_port_tried > last_port) {
      last_server_port_tried =
          first_port + (last_server_port_tried % last_port) - 1;
    }
    std::vector<std::pair<std::string, uint64_t>> response_values;

    std::vector<std::thread> put_threads;
    for (int i = 0; i < write_quorum; i++) {
      put_threads.emplace_back(
          &KVStoreServiceImpl::WriteToServer, this, server_port + i, key, value,
          timestamp, std::ref(put_response_mutex),
          std::ref(last_server_port_tried), std::ref(response_values));
    }

    for (auto &t : put_threads) {
      t.join();
    }

    std::string old_value;
    uint64_t most_recent_timestamp = 0;

    for (const auto &pair : response_values) {
      std::cout << "Response values = " << pair.first << ", " << pair.second
                << std::endl;
      if (pair.second > most_recent_timestamp) {
        old_value = pair.first;
        most_recent_timestamp = pair.second;
      }
    }

    std::cout << "Client PUT Response, old_value, most_recent_timestamp = "
              << old_value << ", " << most_recent_timestamp << std::endl;
    if (old_value != "") {
      response->set_code(0);
      response->set_message(old_value);
    } else {
      response->set_code(1);
    }

    ReplicateRequest async_request;
    async_request.set_key(key);
    async_request.set_value(value);
    async_request.set_timestamp(timestamp);
    async_request.set_async_forward_to_all(false);

    // for (const auto &pair : kvstore_stubs_map) {
    //   if (!replicate_servers_tried.contains(getPortNumber(pair.first))) {
    //     AsyncReplicationHelper(async_request, pair.second);
    //   }
    // }

    return Status::OK;
  }

  void WriteToServer(
      int server_port, const std::string &key, const std::string &value,
      uint64_t timestamp, std::mutex &put_response_mutex,
      std::atomic<int> &last_server_port_tried,
      std::vector<std::pair<std::string, uint64_t>> &response_values) {
    if (server_port > last_port) {
      server_port = first_port + (server_port % last_port) - 1;
    }
    std::string server_address("0.0.0.0:" + std::to_string(server_port));
    ClientContext context_server_put;
    PutRequest replica_put_request;
    PutResponse replica_put_response;

    replica_put_request.set_key(key);
    replica_put_request.set_value(value);
    replica_put_request.set_timestamp(timestamp);
    replica_put_request.set_is_client_request(false);

    Status status = kvstore_stubs_map[server_address]->Put(
        &context_server_put, replica_put_request, &replica_put_response);
    int requests_tried = 1;

    while (requests_tried < write_quorum && !status.ok()) {
      // Retry to some other server
      int next_server_port = ++last_server_port_tried;
      if (next_server_port > last_port) {
        next_server_port = first_port + (next_server_port % last_port) - 1;
      }
      if (next_server_port == server_port) {
        return;
      }

      server_address = "0.0.0.0:" + std::to_string(next_server_port);
      ClientContext context_server_put_retry;
      status = kvstore_stubs_map[server_address]->Put(&context_server_put_retry,
                                                      replica_put_request,
                                                      &replica_put_response);
      requests_tried++;
    }

    if (status.ok()) {
      int response_code = replica_put_response.code();
      if (response_code == 0) {
        put_response_mutex.lock();
        response_values.emplace_back(replica_put_response.message(),
                                     replica_put_response.timestamp());
        put_response_mutex.unlock();
      }
    }
  }

  void
  fetchServerData(int port, const std::string &key,
                  std::atomic<int> &last_server_port_tried,
                  std::mutex &value_mtx, uint64_t &latest_timestamp,
                  std::string &latest_value,
                  std::unordered_map<std::string, uint64_t> &server_timestamps,
                  std::unordered_set<int> &replicate_servers_tried) {
    if (port > last_port) {
      port = first_port + (port % last_port) - 1;
    }
    std::string address("0.0.0.0:" + std::to_string(port));
    ClientContext context_server_get;
    GetReponse get_response;
    GetRequest get_request_for_servers;
    get_request_for_servers.set_is_client_request(false);
    get_request_for_servers.set_key(key);

    Status status = kvstore_stubs_map.at(address)->Get(
        &context_server_get, get_request_for_servers, &get_response);

    int requests_tried = 1;

    while (requests_tried < write_quorum && !status.ok()) {
      // Retry to some other server
      int next_server_port = ++last_server_port_tried;
      if (next_server_port > last_port) {
        next_server_port = first_port + (next_server_port % last_port) - 1;
      }
      if (next_server_port == port) {
        return;
      }
            next_server_port = 50051;
            complete_rev = true;
          }
        }
        replicate_servers_tried.insert(next_server_port);
        mtx.unlock();
      }
      address = "0.0.0.0:" + std::to_string(next_server_port);
      ClientContext context_server_get_retry;
      status = kvstore_stubs_map[address]->Get(
          &context_server_get_retry, get_request_for_servers, &get_response);
      requests_tried++;
    }

    if (status.ok() && get_response.code() == 0) {
      uint64_t timestamp = get_response.timestamp();
      std::string value = get_response.value();
      value_mtx.lock();
      server_timestamps[address] = timestamp;
      // Update the latest timestamp and value if this server has the most
      // recent data
      if (timestamp > latest_timestamp) {
        latest_timestamp = timestamp;
        latest_value = value;
      }
      value_mtx.unlock();
    }
  }

  Status Get(ServerContext *context, const GetRequest *request,
             GetReponse *response) override {

    if (request->is_client_request()) {
      std::cout << "Received client get key = " << request->key() << std::endl;
      std::string server_address = CH.getServer(request->key());
      int port = getPortNumber(server_address);
      std::unordered_set<int> replicate_servers_tried;
      uint64_t latest_timestamp = 0;
      std::string latest_value;
      std::unordered_map<std::string, uint64_t> server_timestamps;
      std::mutex value_mtx;
      std::vector<std::thread> get_threads;
      std::atomic<int> last_server_port_tried = (port + write_quorum - 1);
      if (last_server_port_tried > last_port) {
        last_server_port_tried =
            first_port + (last_server_port_tried % last_port) - 1;
      }

      for (int i = 0; i < write_quorum; i++) {
        get_threads.emplace_back(
            &KVStoreServiceImpl::fetchServerData, this, port + i,
            request->key(), std::ref(last_server_port_tried),
            std::ref(value_mtx), std::ref(latest_timestamp),
            std::ref(latest_value), std::ref(server_timestamps),
            std::ref(replicate_servers_tried));
      }

      for (auto &t : get_threads) {
        t.join();
      }

      if (latest_value != "") {
        response->set_value(latest_value);

        ReplicateRequest async_request;
        async_request.set_key(request->key());
        async_request.set_value(latest_value);
        async_request.set_timestamp(latest_timestamp);
        std::cout << "Sending async repair" << std::endl;
        for (const auto &pair : server_timestamps) {
          const std::string &address = pair.first;
          uint64_t timestamp = pair.second;

          if (timestamp < latest_timestamp) {

            AsyncReplicationHelper(async_request, kvstore_stubs_map[address]);
          }
        }
      } else {
        response->set_code(1);
      }
      return Status::OK;
    } else {
      std::string timestamp_and_value;
      int response_read;
      response_read = kvStore.read(request->key(), timestamp_and_value);

      if (response_read == -1) {
        return grpc::Status(grpc::StatusCode::ABORTED, "");
      }

      response->set_code(response_read);

      if (response_read == 0) {
        std::string value;
        uint64_t timestamp;
        bool parseSuccessful =
            kvStore.parseValue(timestamp_and_value, timestamp, value);
        response->set_value(value);
        response->set_timestamp(timestamp);
      }

      return Status::OK;
    }
  }

  Status Replicate(ServerContext *context, const ReplicateRequest *request,
                   Empty *response) override {

    std::cout << "Received REPLICATE request with key \n";
    std::cout << request->key() << std::endl;
    std::cout << "with async forward: " << request->async_forward_to_all()
              << std::endl;

    std::string old_value;
    int response_write;

    response_write =
        kvStore.write(request->key(), request->value(), 0, old_value);

    if (response_write == 0 || response_write == 1) {
      if (request->async_forward_to_all()) {
        for (auto it = kvstore_stubs_map.begin(); it != kvstore_stubs_map.end();
             it++) {
          ReplicateRequest async_request;
          async_request.set_key(request->key());
          async_request.set_value(request->value());
          async_request.set_async_forward_to_all(false);
          AsyncReplicationHelper(async_request, it->second);
        }
      }
      return Status::OK;
    }
    return grpc::Status(grpc::StatusCode::ABORTED, "");
  }

  Status RestoreServer(ServerContext *context, const RestoreServerRequest *request,
                   RestoreServerResponse *response) override {
    std::cout << "Received Restore Server request \n";
    std::vector<std::pair<std::string, std::string> > restore_keys = kvStore.getAllLatestKeys(
      request->timestamp());

    if (restore_keys.size() == 0) {
      return grpc::Status(grpc::StatusCode::ABORTED, "");
    }

    for (const auto& kv : restore_keys) {
        KeyValuePair* pair = response->add_repair_list();
        pair->set_key(kv.first);
        pair->set_value(kv.second);
    }
    return Status::OK;
  }

  void InitializeServerStubs() {
    for (int port = 50051; port < 50051 + total_servers; port++) {
      std::string address("0.0.0.0:" + std::to_string(port));
      CH.addServer(address);
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

void RunServer(std::string &server_address, int total_servers,
               int virtual_servers_for_ch) {
  int port = std::stoi(server_address.substr(server_address.find(":") + 1,
                                             server_address.size()));
  bool is_primary = port == 50051;
  KVStoreServiceImpl service(server_address, total_servers,
                             virtual_servers_for_ch, is_primary, 3);

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());

  if (!server) {
    std::cerr << "Failed to start server on " << server_address << std::endl;
    exit(1);
  }

  //   std::thread t1(&KVStoreServiceImpl::HeartbeatMechanism, &service);
  std::cout << "Server started at " << server_address << std::endl;
  server->Wait();

  //   t1.join();
}

int main(int argc, char **argv) {
  std::string server_address("0.0.0.0:50051");
  int total_servers = 10;
  if (argc > 1) {
    server_address = argv[1];
    total_servers = std::atoi(argv[2]);
  }

  RunServer(server_address, total_servers, 3);
  return 0;
}
