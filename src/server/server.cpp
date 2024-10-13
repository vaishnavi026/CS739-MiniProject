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
using kvstore::KVStore;
using kvstore::PutRequest;
using kvstore::PutResponse;
using kvstore::ReplicateRequest;

int getPortNumber(const std::string &address)
{
    size_t colon_pos = address.find(':');
    if (colon_pos == std::string::npos)
    {
        throw std::invalid_argument("Invalid address format.");
    }

    return std::stoi(address.substr(colon_pos + 1));
}

bool parseValue(const std::string combined_value, uint64_t &timestamp,
                std::string &value)
{
    // Get delimited position
    size_t delimiter_pos = combined_value.find('|');
    // std::cout << "Combined value " << combined_value << "  " <<
    // std::to_string(delimiter_pos) << std::endl;
    if (delimiter_pos != std::string::npos && delimiter_pos > 0 &&
        delimiter_pos < combined_value.size() - 1)
    {
        timestamp =
            std::stoull(combined_value.substr(0, delimiter_pos)); // get usigned_int
        value = combined_value.substr(delimiter_pos +
                                      1); // Get the value after the delimiter
        return true;
    }

    std::cerr << "Error: Invalid format for combined value" << std::endl;
    return false;
}

void AsyncReplicationHelper(const ReplicateRequest &request,
                            const std::unique_ptr<KVStore::Stub> &stub)
{
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
    std::thread([cq, response, context]()
                {
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
    delete cq; })
        .detach();
}

class KVStoreServiceImpl final : public KVStore::Service
{
private:
    std::mutex change_primary;
    keyValueStore kvStore;
    ConsistentHashing CH;
    std::string server_address;
    int total_servers;
    int virtual_servers_for_ch;
    bool is_primary;
    std::string primary_address;
    int replication_factor;
    std::map<std::string, std::unique_ptr<KVStore::Stub>> kvstore_stubs_map;
    std::chrono::high_resolution_clock::time_point last_heartbeat;

public:
    KVStoreServiceImpl(std::string &server_address, int total_servers,
                       int virtual_servers_for_ch, bool is_primary, int replication_factor)
        : kvStore(server_address), CH(virtual_servers_for_ch)
    {
        this->server_address = server_address;
        this->total_servers = total_servers;
        this->virtual_servers_for_ch = virtual_servers_for_ch;
        this->is_primary = is_primary;
        this->primary_address = "0.0.0.0:50051";
        this->replication_factor = replication_factor;
        if (is_primary)
        {
            InitializeServerStubs();
        }

        for (int port = 50051; port < 50051 + total_servers; port++)
        {
            std::string address("0.0.0.0:" + std::to_string(port));
            CH.addServer(address);
        }
    }

    Status Put(ServerContext *context, const PutRequest *request,
               PutResponse *response) override
    {

        std::string old_timestamp_and_value;
        int response_write;
        response_write =
            kvStore.write(request->key(), request->value(), request->timestamp(),
                          old_timestamp_and_value);

        if (response_write == -1)
        {
            return grpc::Status(grpc::StatusCode::ABORTED, "");
        }

        if (is_primary)
        {
            int sync_replicate_count = 0;

            auto it = kvstore_stubs_map.begin();

            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<> random_func(0, total_servers / 2);
            int rand_server = random_func(gen);

            while (sync_replicate_count < total_servers / 2 &&
                   it != kvstore_stubs_map.end())
            {
                ClientContext context;
                Empty response;
                ReplicateRequest replicate_request;
                replicate_request.set_key(request->key());
                replicate_request.set_value(request->value());

                if (sync_replicate_count == rand_server)
                {
                    replicate_request.set_async_forward_to_all(true);
                }
                else
                {
                    replicate_request.set_async_forward_to_all(false);
                }

                it->second->Replicate(&context, replicate_request, &response);

                it++;
                sync_replicate_count++;
            }
        }

        if (response_write == 0)
        {
            std::string old_value;
            uint64_t timestamp;
            bool parseSuccessful =
                parseValue(old_timestamp_and_value, timestamp, old_value);
            response->set_message(old_value);
        }

        response->set_code(response_write);
        return Status::OK;
    }

    Status Get(ServerContext *context, const GetRequest *request,
               GetReponse *response) override
    {
        if (request->is_client_request())
        {

            int successful_servers_read = 0;
            int R = (replication_factor + 1) / 2;
            std::string server_address = CH.getServer(request->key());
            int port = getPortNumber(server_address);
            int request_count = 0;
            uint64_t latest_timestamp = 0; // keep track of latest read
            std::string latest_value;
            std::unordered_map<std::string, uint64_t> server_timestamps;
            while (successful_servers_read != R)
            {
                std::string address("0.0.0.0:" + std::to_string(port));
                port++;
                std::string value;
                uint64_t timestamp;
                ClientContext context_server_get;
                GetRequest get_request_for_servers;
                get_request_for_servers.set_is_client_request(false);
                get_request_for_servers.set_key(request->key());

                GetReponse server_read_response;
                Status status = kvstore_stubs_map[address]->Get(&context_server_get, get_request_for_servers, &server_read_response);
                if (status.ok() && server_read_response.code() == 0)
                {
                    value = server_read_response.value();
                    timestamp = server_read_response.timestamp();
                    server_timestamps[address] = timestamp;
                    if (timestamp > latest_timestamp)
                    {
                        latest_timestamp = timestamp;
                        latest_value = value;
                    }

                    successful_servers_read += 1;
                }
                if (request_count > 2 * R)
                {
                    // unsuccessful

                    // return Status::CANCELLED;
                    break;
                
                }
            }
            ReplicateRequest replicate_request;
            ClientContext replicate_request_context;
            replicate_request.set_key(request->key());
            replicate_request.set_value(latest_value);
            replicate_request.set_timestamp(latest_timestamp);
            replicate_request.set_async_forward_to_all(false);
            
            for (const auto &pair : server_timestamps)
            {
                const std::string &address = pair.first;
                uint64_t timestamp = pair.second;
                
                Empty response;
                if (timestamp < latest_timestamp)
                {
                    
                    Status replicate_status = kvstore_stubs_map[address]->Replicate(&replicate_request_context, replicate_request, &response);
                }
            }
            response->set_value(latest_value);
            response->set_timestamp(latest_timestamp);
            return Status::OK;
        }
        else
        {
            std::string timestamp_and_value;
            int response_read;
            response_read = kvStore.read(request->key(), timestamp_and_value);

            if (response_read == -1)
            {
                return grpc::Status(grpc::StatusCode::ABORTED, "");
            }

            response->set_code(response_read);

            if (response_read == 0)
            {
                std::string value;
                uint64_t timestamp;
                bool parseSuccessful = parseValue(timestamp_and_value, timestamp, value);
                response->set_value(value);
                response->set_timestamp(timestamp);
            }

            return Status::OK;
        }
    }

    Status Replicate(ServerContext *context, const ReplicateRequest *request,
                     Empty *response) override
    {

        std::cout << "Received REPLICATE request with key \n";
        std::cout << request->key() << std::endl;
        std::cout << "with async forward: " << request->async_forward_to_all()
                  << std::endl;

        std::string old_value;
        int response_write;

        response_write =
            kvStore.write(request->key(), request->value(), 0, old_value);

        if (response_write == 0 || response_write == 1)
        {
            if (request->async_forward_to_all())
            {
                for (auto it = kvstore_stubs_map.begin(); it != kvstore_stubs_map.end();
                     it++)
                {
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

    Status Heartbeat(ServerContext *context, const HeartbeatMessage *request,
                     Empty *response) override
    {
        // std::cout << "Received Heartbeat" << std::endl;
        std::unique_lock<std::mutex> primary_lock(change_primary);
        this->last_heartbeat = std::chrono::high_resolution_clock::now();
        this->primary_address = request->primary();
        return Status::OK;
    }

    void HeartbeatMechanism()
    {
        while (true)
        {
            if (is_primary)
            {
                SendHeartbeats();
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
            }
            else
            {
                CheckLastHeartbeat();
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
    }

    void CheckLastHeartbeat()
    {
        if (last_heartbeat == std::chrono::steady_clock::time_point())
        {
            return;
        }
        std::chrono::high_resolution_clock::time_point current_time =
            std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> duration_milli =
            std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(
                current_time - last_heartbeat);
        // std::cout << "Last heartbeat " << duration_milli.count() << std::endl;
        if (duration_milli.count() > 300)
        {
            std::unique_lock<std::mutex> primary_lock(change_primary);
            this->primary_address = server_address;
            this->is_primary = true;
            InitializeServerStubs();
            SendHeartbeats();
        }
    }

    void SendHeartbeats()
    {
        for (const auto &pair : kvstore_stubs_map)
        {
            // std::cout << pair.first << std::endl;
            HeartbeatMessage message;
            ClientContext context;
            Empty response;

            message.set_primary(server_address);
            pair.second->Heartbeat(&context, message, &response);
        }
    }

    void InitializeServerStubs()
    {
        for (int port = 50051; port < 50051 + total_servers; port++)
        {
            std::string address("0.0.0.0:" + std::to_string(port));
            auto channel =
                grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
            kvstore_stubs_map[address] = kvstore::KVStore::NewStub(channel);
            if (!kvstore_stubs_map[address])
            {
                std::cerr << "Failed to create gRPC stub\n";
            }
            grpc_connectivity_state state = channel->GetState(true);
            if (state == GRPC_CHANNEL_SHUTDOWN ||
                state == GRPC_CHANNEL_TRANSIENT_FAILURE)
            {
                std::cerr << "Failed to establish gRPC channel connection\n";
            }
        }
    }
};

void RunServer(std::string &server_address, int total_servers,
               int virtual_servers_for_ch)
{
    int port = std::stoi(server_address.substr(server_address.find(":") + 1,
                                               server_address.size()));
    bool is_primary = port == 50051;
    KVStoreServiceImpl service(server_address, total_servers,
                               virtual_servers_for_ch, is_primary, 3);

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());

    if (!server)
    {
        std::cerr << "Failed to start server on " << server_address << std::endl;
        exit(1);
    }

    std::thread t1(&KVStoreServiceImpl::HeartbeatMechanism, &service);
    std::cout << "Server started at " << server_address << std::endl;
    server->Wait();

    t1.join();
}

int main(int argc, char **argv)
{
    std::string server_address("0.0.0.0:50051");
    int total_servers = 10;
    if (argc > 1)
    {
        server_address = argv[1];
        total_servers = std::atoi(argv[2]);
    }

    RunServer(server_address, total_servers, 3);
    return 0;
}
