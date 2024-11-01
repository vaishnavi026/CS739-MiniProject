#include "KeyValueController.grpc.pb.h"
#include "KeyValueController.pb.h"
#include "keyValueStore.h"
#include <chrono>
#include <future>
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
using kvstore::HeartbeatMessage;
using kvstore::KeyValuePair;
using kvstore::KVStore;
using kvstore::LeaveRequest;
using kvstore::PutRequest;
using kvstore::PutResponse;
using kvstore::ReplicateRequest;
using kvstore::RestoreServerRequest;
using kvstore::RestoreServerResponse;

int getPortNumber(const std::string &address) {
  size_t colon_pos = address.find(':');
  if (colon_pos == std::string::npos) {
    throw std::invalid_argument("Invalid address format.");
  }

  return std::stoi(address.substr(colon_pos + 1));
}

void AsyncReplicationHelper(const ReplicateRequest &request,
                            const std::unique_ptr<KVStore::Stub> &stub) {
  // return;
  ClientContext *context = new ClientContext;
  Empty *response = new Empty;
  CompletionQueue *cq = new CompletionQueue;
  Status status_;

  std::unique_ptr<grpc::ClientAsyncResponseReader<Empty>> rpc(
      stub->AsyncReplicate(context, request, cq));

  rpc->Finish(response, &status_, (void *)1);
}

class KVStoreServiceImpl final : public KVStore::Service {
private:
  keyValueStore kvStore;
  ConsistentHashing CH;
  std::string server_address;
  int server_port;
  int total_servers;
  int virtual_servers_for_ch;
  int first_port;
  int last_port;
  int replication_factor;
  int read_write_quorum;
  bool accept_request;
  std::map<std::string, std::unique_ptr<KVStore::Stub>> kvstore_stubs_map;

public:
  KVStoreServiceImpl(std::string &server_address, int total_servers,
                     int virtual_servers_for_ch, int replication_factor)
      : kvStore(server_address), CH(virtual_servers_for_ch) {
    this->server_address = server_address;
    this->server_port = getPortNumber(server_address);
    this->total_servers = total_servers;
    this->virtual_servers_for_ch = virtual_servers_for_ch;
    this->first_port = 50051;
    this->last_port = first_port + 100 - 1;
    this->replication_factor = replication_factor;
    this->read_write_quorum = (replication_factor + 1) / 2;
    this->accept_request = true;
    if (this->total_servers == -1) {
      InitializeServerStubs(this->last_port);
      AnnounceStartToAll();
      this->accept_request = false;
      std::thread recovery_thread([this]() { HandleFailedMachineRecovery(); });

      recovery_thread.detach();
    } else {
      std::cout << "Started a new set of instances with count = "
                << total_servers << std::endl;
      InitializeServerStubs(first_port + total_servers - 1);
    }

    // if (isRecoveredServer()) {
    //     HandleFailedMachineRecovery();
    // }
  }

  Status Put(ServerContext *context, const PutRequest *request,
             PutResponse *response) override {
    if (accept_request == false) {
      return grpc::Status(grpc::StatusCode::ABORTED, "");
    }

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
      // std::cout << "Completed write for another server" << std::endl;
      return Status::OK;
    }
    std::string hashed_server = CH.getServer(key);
    int hashed_server_port = getPortNumber(hashed_server);

    auto now = std::chrono::system_clock::now();
    timestamp = std::chrono::duration_cast<std::chrono::nanoseconds>(
                    now.time_since_epoch())
                    .count();

    std::mutex put_response_mutex;
    std::vector<std::pair<std::string, uint64_t>> response_values;

    std::vector<std::future<bool>> futures;
    std::unordered_set<int> ports_tried;
    int server_port;
    std::string server_address;
    std::string next_server = hashed_server;
    // std::cout << "Hashed server port for PUT key = " << hashed_server_port
    // << std::endl;

    for (int i = 0; i < read_write_quorum; i++) {
      // server_port = hashed_server_port + i;
      // if (server_port > last_port) {
      //   server_port = first_port + (server_port % last_port) - 1;
      // }

      server_address = next_server;
      next_server = CH.getServer(next_server);
      server_port = getPortNumber(server_address);

      // server_address = std::string("127.0.0.1:") +
      // std::to_string(server_port);
      if (kvstore_stubs_map.contains(server_address) &&
          !ports_tried.contains(server_port)) {
        // std::cout << "Trying to send a server write to " << server_port
        //           << std::endl;
        futures.push_back(std::async(
            std::launch::async, &KVStoreServiceImpl::WriteToServer, this,
            server_port, key, value, timestamp, std::ref(put_response_mutex),
            std::ref(response_values)));
      }
      ports_tried.insert(server_port);
    }

    // std::cout << "Created all write thread futures" << std::endl;

    int retry_count = 0;
    int max_retry_count = 100;
    int successful_writes = 0;

    while (successful_writes < read_write_quorum &&
           retry_count < max_retry_count &&
           ports_tried.size() < kvstore_stubs_map.size()) {
      for (int i = 0; i < futures.size(); i++) {
        std::future_status status;
        auto &f = futures[i];
        if (f.valid()) {
          status = f.wait_for(std::chrono::seconds(0));
          if (status == std::future_status::ready) {
            bool result = f.get();
            if (!result) {
              while (ports_tried.size() < kvstore_stubs_map.size() &&
                     ports_tried.contains(server_port)) {
                server_address = next_server;
                next_server = CH.getServer(next_server);
                server_port = getPortNumber(server_address);
                // server_port += 1;
                // if (server_port > last_port) {
                //   server_port = first_port + (server_port % last_port) - 1;
                // }
              }

              // server_address =
              //     std::string("127.0.0.1:") + std::to_string(server_port);
              if (kvstore_stubs_map.contains(server_address)) {
                futures[i] = std::move(std::async(
                    std::launch::async, &KVStoreServiceImpl::WriteToServer,
                    this, server_port, key, value, timestamp,
                    std::ref(put_response_mutex), std::ref(response_values)));
                retry_count += 1;
                // std::cout << "Retrying write thread future for " <<
                // server_port
                //           << std::endl;
              }
              ports_tried.insert(server_port);
            } else {
              successful_writes += 1;
            }
          }
        }
      }
    }
    // std::cout << "Received successful_writes value" << std::endl;
    if (successful_writes < read_write_quorum) {
      for (auto &f : futures) {
        if (f.valid()) {
          f.wait();
          if (f.get()) {
            successful_writes += 1;
          }
        }
      }
    }

    int response_code;

    if (successful_writes == 0) {
      response_code = -1;
    } else if (successful_writes < read_write_quorum) {
      response_code = -2;
    } else {
      std::string old_value;
      uint64_t most_recent_timestamp = 0;

      for (const auto &pair : response_values) {
        if (pair.second > most_recent_timestamp) {
          old_value = pair.first;
          most_recent_timestamp = pair.second;
        }
      }
      if (old_value != "") {
        response_code = 0;
        response->set_message(old_value);
      } else {
        response_code = 1;
      }

      ReplicateRequest async_request;
      async_request.set_key(key);
      async_request.set_value(value);
      async_request.set_timestamp(timestamp);
      async_request.set_async_forward_to_all(false);
      std::string server_address;

      for (int p = first_port; p <= last_port; p++) {
        if (!ports_tried.contains(p)) {
          server_address = std::string("127.0.0.1:") + std::to_string(p);
          if (kvstore_stubs_map.contains(server_address)) {
            AsyncReplicationHelper(async_request,
                                   kvstore_stubs_map[server_address]);
          }
        }
      }
    }
    response->set_code(response_code);
    return Status::OK;
  }

  bool WriteToServer(
      int server_port, const std::string &key, const std::string &value,
      uint64_t timestamp, std::mutex &put_response_mutex,
      std::vector<std::pair<std::string, uint64_t>> &response_values) {
    std::string server_address("127.0.0.1:" + std::to_string(server_port));
    ClientContext context_server_put;
    PutRequest replica_put_request;
    PutResponse replica_put_response;

    replica_put_request.set_key(key);
    replica_put_request.set_value(value);
    replica_put_request.set_timestamp(timestamp);
    replica_put_request.set_is_client_request(false);

    Status status = kvstore_stubs_map[server_address]->Put(
        &context_server_put, replica_put_request, &replica_put_response);

    if (status.ok()) {
      int response_code = replica_put_response.code();
      if (response_code == 0) {
        std::lock_guard<std::mutex> lock(put_response_mutex);
        response_values.emplace_back(replica_put_response.message(),
                                     replica_put_response.timestamp());
      }
      return true;
    }
    if (!status.ok() && status.error_code() != grpc::StatusCode::ABORTED) {
      kvstore_stubs_map.erase(server_address);
      CH.removeServer(server_address);
    }
    return false;
  }

  bool FetchServerData(
      int server_port, const std::string &key, std::mutex &value_mtx,
      uint64_t &latest_timestamp, std::string &latest_value,
      std::unordered_map<std::string, uint64_t> &server_timestamps) {
    std::string address("127.0.0.1:" + std::to_string(server_port));
    ClientContext context_server_get;
    GetReponse get_response;
    GetRequest get_request_for_servers;
    get_request_for_servers.set_is_client_request(false);
    get_request_for_servers.set_key(key);

    Status status = kvstore_stubs_map[address]->Get(
        &context_server_get, get_request_for_servers, &get_response);

    if (status.ok()) {
      if (get_response.code() == 0) {
        uint64_t timestamp = get_response.timestamp();
        std::string value = get_response.value();
        std::lock_guard<std::mutex> lock(value_mtx);
        server_timestamps[address] = timestamp;
        // Update the latest timestamp and value if this server has the most
        // recent data
        if (timestamp > latest_timestamp) {
          latest_timestamp = timestamp;
          latest_value = value;
        }
      }
      return true;
    }
    if (!status.ok() && status.error_code() != grpc::StatusCode::ABORTED) {
      kvstore_stubs_map.erase(address);
      CH.removeServer(address);
    }
    return false;
  }

  Status Get(ServerContext *context, const GetRequest *request,
             GetReponse *response) override {

    if (accept_request == false) {
      return grpc::Status(grpc::StatusCode::ABORTED, "");
    }

    std::string key = request->key();

    if (request->is_client_request()) {
      std::string hashed_server = CH.getServer(key);
      int hashed_server_port = getPortNumber(hashed_server);

      uint64_t latest_timestamp = 0;
      std::string latest_value;
      std::unordered_map<std::string, uint64_t> server_timestamps;
      std::mutex value_mtx;

      std::vector<std::future<bool>> futures;
      std::unordered_set<int> ports_tried;
      int server_port;
      std::string server_address;
      std::string next_server = hashed_server;
      // std::cout << "Hashed server port for GET key = " << hashed_server_port
      //           << std::endl;
      for (int i = 0; i < read_write_quorum; i++) {
        // server_port = hashed_server_port + i;
        // if (server_port > last_port) {
        //   server_port = first_port + (server_port % last_port) - 1;
        // }

        server_address = next_server;
        next_server = CH.getServer(next_server);
        server_port = getPortNumber(server_address);

        // server_address =
        //     std::string("127.0.0.1:") + std::to_string(server_port);
        if (kvstore_stubs_map.contains(server_address) &&
            !ports_tried.contains(server_port)) {
          // std::cout << "Trying to send a server get to " << server_port
          //           << std::endl;
          futures.push_back(std::async(
              std::launch::async, &KVStoreServiceImpl::FetchServerData, this,
              server_port, key, std::ref(value_mtx), std::ref(latest_timestamp),
              std::ref(latest_value), std::ref(server_timestamps)));
        }
        ports_tried.insert(server_port);
      }

      // std::cout << "Created all get thread futures" << std::endl;

      int retry_count = 0;
      int max_retry_count = 100;
      int successful_reads = 0;

      while (successful_reads < read_write_quorum &&
             retry_count < max_retry_count &&
             ports_tried.size() < kvstore_stubs_map.size()) {
        for (int i = 0; i < futures.size(); i++) {
          std::future_status status;
          auto &f = futures[i];
          if (f.valid()) {
            status = f.wait_for(std::chrono::seconds(0));
            if (status == std::future_status::ready) {
              bool result = f.get();
              if (!result) {
                while (ports_tried.size() < kvstore_stubs_map.size() &&
                       ports_tried.contains(server_port)) {
                  server_address = next_server;
                  next_server = CH.getServer(next_server);
                  server_port = getPortNumber(server_address);
                  // server_port += 1;
                  // if (server_port > last_port) {
                  //   server_port = first_port + (server_port % last_port) - 1;
                  // }
                }

                // server_address =
                //     std::string("127.0.0.1:") + std::to_string(server_port);

                if (kvstore_stubs_map.contains(server_address)) {
                  futures[i] = std::move(std::async(
                      std::launch::async, &KVStoreServiceImpl::FetchServerData,
                      this, server_port, key, std::ref(value_mtx),
                      std::ref(latest_timestamp), std::ref(latest_value),
                      std::ref(server_timestamps)));
                  retry_count += 1;
                  // std::cout << "Retrying get thread future for " <<
                  // server_port
                  //           << std::endl;
                }
                ports_tried.insert(server_port);
              } else {
                successful_reads += 1;
              }
            }
          }
        }
      }
      // std::cout << "Received successful_reads value" << std::endl;
      if (successful_reads < read_write_quorum) {
        for (auto &f : futures) {
          if (f.valid()) {
            f.wait();
            if (f.get()) {
              successful_reads += 1;
            }
          }
        }
      }

      int response_code = 1;

      if (successful_reads == 0) {
        response_code = -1;
      }

      if (latest_value != "") {
        response_code = 0;
        response->set_value(latest_value);

        ReplicateRequest async_request;
        async_request.set_key(key);
        async_request.set_value(latest_value);
        async_request.set_timestamp(latest_timestamp);

        for (const auto &pair : server_timestamps) {
          const std::string &address = pair.first;
          uint64_t timestamp = pair.second;
          if (timestamp < latest_timestamp) {
            AsyncReplicationHelper(async_request, kvstore_stubs_map[address]);
          }
        }
      }

      response->set_code(response_code);
      return Status::OK;
    } else {
      std::string timestamp_and_value;
      int response_read;
      response_read = kvStore.read(key, timestamp_and_value);

      if (response_read == -1) {
        return grpc::Status(grpc::StatusCode::ABORTED, "");
      }

      response->set_code(response_read);

      if (response_read == 0) {
        std::string value;
        uint64_t timestamp;
        kvStore.parseValue(timestamp_and_value, timestamp, value);
        response->set_value(value);
        response->set_timestamp(timestamp);
      }
      // std::cout << "Completed read for another server" << std::endl;
      return Status::OK;
    }
  }

  Status Replicate(ServerContext *context, const ReplicateRequest *request,
                   Empty *response) override {
    std::string old_value;
    int response_write;

    response_write = kvStore.write(request->key(), request->value(),
                                   request->timestamp(), old_value);

    if (response_write == 0 || response_write == 1) {
      return Status::OK;
    }
    return grpc::Status(grpc::StatusCode::ABORTED, "");
  }

  Status RestoreServer(ServerContext *context,
                       const RestoreServerRequest *request,
                       RestoreServerResponse *response) override {
    std::vector<std::pair<std::string, std::string>> restore_keys =
        kvStore.getAllLatestKeys(request->timestamp());

    if (restore_keys.size() == 0) {
      return grpc::Status(grpc::StatusCode::ABORTED, "");
    }

    for (const auto &kv : restore_keys) {
      KeyValuePair *pair = response->add_repair_list();
      pair->set_key(kv.first);
      pair->set_value(kv.second);
    }
    return Status::OK;
  }

  Status Heartbeat(ServerContext *context, const HeartbeatMessage *request,
                   Empty *response) override {
    if (request->is_new()) {
      std::string server_address("127.0.0.1:" +
                                 std::to_string(request->server_port()));
      CH.addServer(server_address);
      auto channel = grpc::CreateChannel(server_address,
                                         grpc::InsecureChannelCredentials());
      kvstore_stubs_map[server_address] = kvstore::KVStore::NewStub(channel);
      this->total_servers += 1;
    } else {
      std::string server_address("127.0.0.1:" +
                                 std::to_string(request->server_port()));
      CH.removeServer(server_address);
      kvstore_stubs_map.erase(server_address);
      this->total_servers -= 1;
    }
    return Status::OK;
  }

  void InitializeServerStubs(int last_port) {
    for (int port = first_port; port <= last_port; port++) {
      std::string address("127.0.0.1:" + std::to_string(port));
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
    // std::cout << "Completed InitializeServerStubs" << std::endl;
    // CH.printServersRing();
  }

  void AnnounceStartToAll() {
    HeartbeatMessage request;
    Empty response;

    request.set_is_new(true);
    request.set_server_port(server_port);
    std::vector<std::string> inactive_servers;

    for (auto const &server_and_stub : kvstore_stubs_map) {
      ClientContext context;
      Status status =
          server_and_stub.second->Heartbeat(&context, request, &response);
      if (!status.ok() && server_and_stub.first != server_address) {
        CH.removeServer(server_and_stub.first);
        inactive_servers.push_back(server_and_stub.first);
      }
    }

    for (std::string const &address : inactive_servers) {
      kvstore_stubs_map.erase(address);
    }
    std::cout << "Completed Announce To All" << std::endl;
    // CH.printServersRing();
  }

  void PutServerInitKey() {
    std::string server_init_key = "#";
    std::string server_init_value = "";
    std::uint64_t server_init_timestamp;
    std::string old_timestamp_and_value;

    auto now = std::chrono::system_clock::now();
    server_init_timestamp =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            now.time_since_epoch())
            .count();

    int response_write =
        kvStore.write(server_init_key, server_init_value, server_init_timestamp,
                      old_timestamp_and_value);
    if (response_write == -1) {
      std::cerr << "Failed to put server init key in DB" << std::endl;
      exit(1);
    }
  }

  bool isRecoveredServer() {
    std::string failed_server_check_key = "#";
    std::string timestamp_and_value;
    int response_read =
        kvStore.read(failed_server_check_key, timestamp_and_value);

    if (response_read == 1) {
      PutServerInitKey();
      return 0;
    } else if (response_read == -1) {
      std::cerr << "Server with addr = " << this->server_address
                << " has db error on reading # key" << std::endl;
      exit(1);
    }
    return 1;
  }

  void HandleFailedMachineRecovery() {
    std::cout << "Machine Recovery Started" << std::endl;
    int server_port;
    std::vector<std::pair<std::string, std::string>> kv_vector;
    uint64_t last_written_timestamp;
    std::string recovery_request_timestamp_value = "";
    std::string last_written_value;
    std::string latest_timestamp_key = "$";
    std::string consistent_hash_address = CH.getServer(this->server_address);

    RestoreServerRequest handle_recovery_request;
    RestoreServerResponse recovery_response;

    int port_number = getPortNumber(consistent_hash_address);
    int response_read =
        kvStore.read(latest_timestamp_key, recovery_request_timestamp_value);

    // To handle the condition of after which timestamp the values are needed
    // for a DB.
    if (response_read == 1 && recovery_request_timestamp_value == "") {
      auto lowest_time = std::chrono::system_clock::time_point::min();
      last_written_timestamp =
          std::chrono::duration_cast<std::chrono::nanoseconds>(
              lowest_time.time_since_epoch())
              .count();
    } else {
      last_written_timestamp = std::stoull(recovery_request_timestamp_value);
    }

    for (int i = port_number; i < port_number + total_servers; i++) {

      int server_port;
      if (i >= first_port + total_servers) {
        server_port = first_port + (i % last_port);
      } else {
        server_port = i;
      }

      std::string server_address("127.0.0.1:" + std::to_string(server_port));

      ClientContext handle_recovery_context;

      handle_recovery_request.set_timestamp(last_written_timestamp);
      Status status = kvstore_stubs_map[server_address]->RestoreServer(
          &handle_recovery_context, handle_recovery_request,
          &recovery_response);

      if (status.ok()) {
        break;
      }
    }

    for (int i = 0; i < recovery_response.repair_list_size(); ++i) {
      const KeyValuePair &kv_pair = recovery_response.repair_list(i);
      kv_vector.push_back({kv_pair.key(), kv_pair.value()});
    }

    int batched_write_status = kvStore.batched_write(kv_vector);
    if (batched_write_status == -1) {
      std::cerr << "Batched write error" << std::endl;
    }

    this->accept_request = true;
  }

  void ExitServerDelayed() {
    sleep(15);
    exit(1);
  }

  Status Leave(ServerContext *context, const LeaveRequest *request,
               Empty *response) override {
    int clean_code = request->clean();
    accept_request = false;
    if (clean_code == 1) {
      HeartbeatMessage request;
      Empty response;

      request.set_is_new(false);
      request.set_server_port(server_port);
      for (auto const &server_and_stub : kvstore_stubs_map) {
        ClientContext context;
        Status status =
            server_and_stub.second->Heartbeat(&context, request, &response);
      }
      std::cout << "Flushing state, server shutting down soon";
      sleep(15);
    } else {
      std::cout << "Server killed";
    }

    std::thread exitThread(&KVStoreServiceImpl::ExitServerDelayed, this);
    return Status::OK;
  }

  Status Die(ServerContext *context, const DieRequest *request,
             Empty *response) override {
    int clean_code = request->clean();
    if (clean_code == 1) {
      // TODO: If primary, complete state replciation and new election.
      std::cout << "Flushing state, server shutting down soon";
      sleep(15);
    } else {
      std::cout << "Server killed";
    }

    accept_request = false;

    // exit(1);
    return Status::OK;
  }

  Status Restart(ServerContext *context, const Empty *request,
                 Empty *response) override {

    HandleFailedMachineRecovery();
    accept_request = true;

    return Status::OK;
  }
};

void RunServer(std::string &server_address, int total_servers,
               int virtual_servers_for_ch) {
  int port = std::stoi(server_address.substr(server_address.find(":") + 1,
                                             server_address.size()));
  KVStoreServiceImpl service(server_address, total_servers,
                             virtual_servers_for_ch, 5);

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
  std::string server_address("127.0.0.1:50051");
  int total_servers = -1;
  if (argc > 1) {
    server_address = argv[1];
    if (argc == 3) {
      total_servers = std::atoi(argv[2]);
    }
  }

  RunServer(server_address, total_servers, 1);
  return 0;
}
