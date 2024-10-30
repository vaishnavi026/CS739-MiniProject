#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <stdlib.h>
#include <cassert>
#include <time.h>
#include <atomic>
#include <thread>
#include <map>
#include <set>
#include <vector>
#include <algorithm>

#include "kv739.h"

// #include <absl/flags/flag.h>
// #include <absl/flags/parse.h>

// ABSL_FLAG(std::string, config, "config", "The path to the config file used to launch this server");

void run_get_requests(int key_value, std::map<int, int>& server_state) {
    // Determine the expected values
    std::string expected_value = "";
    int expected_return_code = 1;

    // See if the key is present
    if(server_state.find(key_value) != server_state.end()) {
        expected_return_code = 0;
        expected_value = std::to_string(server_state[key_value]);
    }

    // Now make the request
    char key[256], value[256];
    snprintf(key, sizeof(key), "%d", key_value);
    int returned_code = kv739_get(key, value);

    // Check that we got the expected and if we were expecting a value that the value is the same
    if(returned_code != expected_return_code) {
        std::cerr << "ERROR: Get request for key " << key_value << " returned code " << returned_code << " but was expecting " << expected_return_code << std::endl;
        exit(1);
    }
    
    if(expected_return_code == 0 && std::string(value) != expected_value) {
        std::cerr << "ERROR: Get request for key " << key_value << " returned value " << value << " but was expecting " << expected_value << std::endl;
        exit(1);
    }
}

void run_put_requests(int key_value, int put_value, std::map<int, int>& server_state) {
    // Determine the expected values
    std::string expected_value = "";
    int expected_return_code = 1;

    // See if the key is present
    if(server_state.find(key_value) != server_state.end()) {
        expected_return_code = 0;
        expected_value = std::to_string(server_state[key_value]);
    }
    server_state[key_value] = put_value;

    // Now make the request
    char key[256], value[256], old_value[256];
    snprintf(key, sizeof(key), "%d", key_value);
    snprintf(value, sizeof(value), "%d", put_value);
    int returned_code = kv739_put(key, value, old_value);

    // Check that we got the expected and if we were expecting a value that the value is the same
    std::string put_request = "(" + std::to_string(key_value) + "," + std::to_string(put_value) + ")";
    if(returned_code != expected_return_code) {
        std::cerr << "ERROR: Put request for kv pair " << put_request << " returned code " << returned_code << " but was expecting " << expected_return_code << std::endl;
        exit(1);
    }
    
    if(expected_return_code == 0 && std::string(old_value) != expected_value) {
        std::cerr << "ERROR: Put request for kv pair " << put_request << " returned value " << old_value << " but was expecting " << expected_value << std::endl;
        exit(1);
    }
}

const int value_range[] = {1, 10000};
void run_simulation(int num_requests, double put_weight, std::map<int, int>& server_state) {
    std::random_device rd; 
    std::mt19937 generator(rd()); 
    std::uniform_int_distribution<> distribution(value_range[0], value_range[1]);

    for(int i = 0; i < num_requests; i++) {
        int curr_key = distribution(generator);
        double random_value = ((double) rand()) / RAND_MAX;
        if(random_value < put_weight) {
            int curr_value = distribution(generator);
            run_put_requests(curr_key, curr_value, server_state);
        } else {
            run_get_requests(curr_key, server_state);
        }
    }
}

void do_membership_changes(std::string config_file,int first_port,int last_port, int max_new_members_added,int max_old_members_deleted,  std::atomic<bool>& test_currently_running) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::vector<std::string> member_servers;
    std::vector<std::string> not_member_not_left_servers;
    // You'll need to implement a way to get all server addresses from the config file
    // For now, I'll assume you have a function to do this
    member_servers = get_servers_from_config(config_file);

    std::set<std::string> old_members;

    while(test_currently_running.load()) {
        std::vector<std::string> running_servers = all_servers;
        for (const auto& server : killed_servers) {
            running_servers.erase(std::remove(running_servers.begin(), running_servers.end(), server), running_servers.end());
        }

        double random_value = ((double) rand()) / RAND_MAX;
        if (random_value < 0.25 && !running_servers.empty() && killed_servers.size() < static_cast<size_t>(max_kill_servers)) {
            std::uniform_int_distribution<> dis(0, running_servers.size() - 1);
            int index = dis(gen);
            std::string server_to_kill = running_servers[index];

            if(kv739_die(const_cast<char*>(server_to_kill.c_str())) == 0) {
                killed_servers.insert(server_to_kill);
            }
        } else if (!killed_servers.empty()) {
            std::vector<std::string> killed_servers_vec(killed_servers.begin(), killed_servers.end());
            std::uniform_int_distribution<> dis(0, killed_servers_vec.size() - 1);
            int index = dis(gen);
            std::string server_to_restart = killed_servers_vec[index];
            
            if(kv739_restart(const_cast<char*>(server_to_restart.c_str())) == 0) {
                killed_servers.erase(server_to_restart);
            }
        }

        std::uniform_int_distribution<> sleep_dis(2500, 7500);
        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_dis(gen)));
    }
}

const int requests_range[] = {1000, 2500};
int main(int argc, char** argv) {
    srand(time(NULL));
    // absl::ParseCommandLine(argc, argv);
    // std::string target_config = absl::GetFlag(FLAGS_config);
    std::string target_config = argv[1];

    if (kv739_init(const_cast<char*>(target_config.c_str())) != 0) {
        std::cerr << "Failed to initialize KV store" << std::endl;
        return 1;
    }

    std::map<int, int> server_state;

    std::atomic<bool> test_currently_running(true);
    // int quroum_reads = std::stoi(std::string(std::getenv("kv_quoroum_reads")));
    // int quorum_writes = std::stoi(std::string(std::getenv("kv_quorum_writes")));
    int quroum_reads = std::stoi(std::string(std::getenv("kv_quoroum_reads")));
    int quorum_writes = std::stoi(std::string(std::getenv("kv_quorum_writes")));
    int max_kill_servers = std::min(quroum_reads, quorum_writes) - 1;
    std::thread membership_changes_thread(do_membership_changes, target_config, max_new_members_added, max_old_members_deleted, std::ref(test_currently_running));

    int num_simulations = 5;
    std::random_device rd; 
    std::mt19937 generator(rd()); 
    std::uniform_int_distribution<> distribution(requests_range[0], requests_range[1]);
    for(int i = 0; i < num_simulations; i++) {
        int num_requests = distribution(generator);
        double put_weight = ((double) rand()) / RAND_MAX;
        std::cout << "Simulation " << i << " makes " << num_requests << " requests with put probability of " << put_weight << std::endl;
        run_simulation(num_requests, put_weight, server_state);
    }
    std::cout << "PASSED: Got correct result for all simulations" << std::endl;    

    test_currently_running.store(false);
    restarting_thread.join();
    kv739_shutdown();
    return 0;
}
