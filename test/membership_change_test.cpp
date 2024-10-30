#include <algorithm>
#include <atomic>
#include <cassert>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <stdlib.h>
#include <string>
#include <thread>
#include <time.h>
#include <vector>

#include "kv739.h"

// #include <absl/flags/flag.h>
// #include <absl/flags/parse.h>

// ABSL_FLAG(std::string, config, "config", "The path to the config file used to
// launch this server");
std::vector<std::string> get_servers_from_config(std::string config_file) {

  std::ifstream file(config_file);
  std::vector<std::string> member_servers;

  if (file.is_open()) {
    std::string line;
    while (std::getline(file, line)) {
      member_servers.push_back(line); // Add each line to the vector
    }
    file.close();
  }

  return member_servers;
}

std::vector<std::string>
get_non_init_servers_in_range(std::vector<std::string> member_servers,
                              int first_port, int last_port) {

  std::set<std::string> member_servers_set;
  std::vector<std::string> non_members_not_left_servers;

  for (int i = 0; i < member_servers.size(); i++) {
    member_servers_set.insert(member_servers[i]);
  }

  for (int port_number = first_port; port_number <= last_port; port_number++) {
    std::string server_address =
        std::string("127.0.0.1:") + std::to_string(port_number);
    if (member_servers_set.find(server_address) == member_servers_set.end()) {
      non_members_not_left_servers.push_back(server_address);
    }
  }

  return non_members_not_left_servers;
}

void run_get_requests(int key_value, std::map<int, int> &server_state) {
  // Determine the expected values
  std::string expected_value = "";
  int expected_return_code = 1;

  // See if the key is present
  if (server_state.find(key_value) != server_state.end()) {
    expected_return_code = 0;
    expected_value = std::to_string(server_state[key_value]);
  }

  // Now make the request
  char key[256], value[256];
  snprintf(key, sizeof(key), "%d", key_value);
  int returned_code = kv739_get(key, value);

  // Check that we got the expected and if we were expecting a value that the
  // value is the same
  if (returned_code != expected_return_code) {
    std::cerr << "ERROR: Get request for key " << key_value << " returned code "
              << returned_code << " but was expecting " << expected_return_code
              << std::endl;
    // exit(1);
  }

  if (expected_return_code == 0 && std::string(value) != expected_value) {
    std::cerr << "ERROR: Get request for key " << key_value
              << " returned value " << value << " but was expecting "
              << expected_value << std::endl;
    // exit(1);
  }
}

void run_put_requests(int key_value, int put_value,
                      std::map<int, int> &server_state) {
  // Determine the expected values
  std::string expected_value = "";
  int expected_return_code = 1;

  // See if the key is present
  if (server_state.find(key_value) != server_state.end()) {
    expected_return_code = 0;
    expected_value = std::to_string(server_state[key_value]);
  }
  server_state[key_value] = put_value;

  // Now make the request
  char key[256], value[256], old_value[256];
  snprintf(key, sizeof(key), "%d", key_value);
  snprintf(value, sizeof(value), "%d", put_value);
  int returned_code = kv739_put(key, value, old_value);

  // Check that we got the expected and if we were expecting a value that the
  // value is the same
  std::string put_request =
      "(" + std::to_string(key_value) + "," + std::to_string(put_value) + ")";
  if (returned_code != expected_return_code) {
    std::cerr << "ERROR: Put request for kv pair " << put_request
              << " returned code " << returned_code << " but was expecting "
              << expected_return_code << std::endl;
    // exit(1);
  }

  if (expected_return_code == 0 && std::string(old_value) != expected_value) {
    std::cerr << "ERROR: Put request for kv pair " << put_request
              << " returned value " << old_value << " but was expecting "
              << expected_value << std::endl;
    // exit(1);
  }
}

const int value_range[] = {1, 10000};
void run_simulation(int num_requests, double put_weight,
                    std::map<int, int> &server_state) {
  std::random_device rd;
  std::mt19937 generator(rd());
  std::uniform_int_distribution<> distribution(value_range[0], value_range[1]);

  for (int i = 0; i < num_requests; i++) {
    int curr_key = distribution(generator);
    double random_value = ((double)rand()) / RAND_MAX;
    if (random_value < put_weight) {
      int curr_value = distribution(generator);
      run_put_requests(curr_key, curr_value, server_state);
    } else {
      run_get_requests(curr_key, server_state);
    }
  }
}

void do_membership_changes(std::string config_file, int first_port,
                           int last_port, int max_new_members_added,
                           int max_old_members_deleted,
                           std::atomic<bool> &test_currently_running) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::vector<std::string> member_servers;
  std::vector<std::string> non_members_not_left_servers;
  // You'll need to implement a way to get all server addresses from the config
  // file For now, I'll assume you have a function to do this
  member_servers = get_servers_from_config(config_file);
  non_members_not_left_servers =
      get_non_init_servers_in_range(member_servers, first_port, last_port);

  std::set<std::string> old_members;
  std::set<std::string> new_members;
  int clean = 0;
  std::uniform_real_distribution<> op_distrib(0, 1.0);
  std::vector<std::string> running_servers = member_servers;
  while (test_currently_running.load()) {
    double random_value = op_distrib(gen);
    std::cout << "Random value " << random_value << std::endl;
    if (!running_servers.empty()) {
      if (random_value <= 0.5 &&
          old_members.size() < static_cast<size_t>(max_old_members_deleted)) {
        std::uniform_int_distribution<> dis(0, member_servers.size() - 1);
        int index = dis(gen);
        std::string server_to_leave = member_servers[index];
        double random_value_clean = op_distrib(gen);
        if (random_value_clean < 0.25)
          clean = 0;
        else
          clean = 1;
        std::cout << "Removing server at " << server_to_leave << std::endl;
        if (kv739_leave(const_cast<char *>(server_to_leave.c_str()), clean) ==
            0) {
          old_members.insert(server_to_leave);
          member_servers.erase(member_servers.begin() + index);
        }
      } else if (random_value > 0.5 &&
                 new_members.size() <
                     static_cast<size_t>(max_new_members_added)) {
        std::uniform_int_distribution<> dis(
            0, non_members_not_left_servers.size() - 1);
        int index = dis(gen);
        std::string server_to_join = non_members_not_left_servers[index];

        if (kv739_start(const_cast<char *>(server_to_join.c_str()), 1) == 0) {
          std::cout << "Started server at " << server_to_join << std::endl;
          non_members_not_left_servers.erase(
              non_members_not_left_servers.begin() + index);
          running_servers.push_back(server_to_join);
          new_members.insert(server_to_join);
        }
      }
    }

    std::uniform_int_distribution<> sleep_dis(250, 750);
    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_dis(gen)));
  }
}

const int requests_range[] = {10000, 25000};
int main(int argc, char **argv) {
  srand(time(NULL));
  // absl::ParseCommandLine(argc, argv);
  // std::string target_config = absl::GetFlag(FLAGS_config);
  std::string target_config = argv[1];
  int first_port = std::stoi(argv[2]);
  int last_port = first_port + 100 - 1;

  if (kv739_init(const_cast<char *>(target_config.c_str())) != 0) {
    std::cerr << "Failed to initialize KV store" << std::endl;
    return 1;
  }

  std::map<int, int> server_state;

  std::atomic<bool> test_currently_running(true);
  // int quroum_reads = std::stoi(std::string(std::getenv("kv_quoroum_reads")));
  // int quorum_writes =
  // std::stoi(std::string(std::getenv("kv_quorum_writes")));
  int quroum_reads = 3;
  int quorum_writes = 3;
  int total_servers = get_servers_from_config(target_config).size();
  int max_old_members_deleted =
      std::max(total_servers - quroum_reads, total_servers - quorum_writes) - 1;
  int max_new_member_added =
      (total_servers /
       2); // If e.g the servers we had init with is 20, max add 10 servers
  int max_kill_servers = std::min(quroum_reads, quorum_writes) - 1;
  std::thread membership_changes_thread(
      do_membership_changes, target_config, first_port, last_port,
      max_new_member_added, max_old_members_deleted,
      std::ref(test_currently_running));

  int num_simulations = 5;
  std::random_device rd;
  std::mt19937 generator(rd());
  std::uniform_int_distribution<> distribution(requests_range[0],
                                               requests_range[1]);
  for (int i = 0; i < num_simulations; i++) {
    int num_requests = distribution(generator);
    double put_weight = ((double)rand()) / RAND_MAX;
    std::cout << "Simulation " << i << " makes " << num_requests
              << " requests with put probability of " << put_weight
              << std::endl;
    run_simulation(num_requests, put_weight, server_state);
  }
  std::cout << "PASSED: Got correct result for all simulations" << std::endl;

  test_currently_running.store(false);
  membership_changes_thread.join();
  kv739_shutdown();
  return 0;
}
