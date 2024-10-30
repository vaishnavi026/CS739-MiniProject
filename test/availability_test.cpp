#include "client_interface.h"
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstring>
#include <ctime>
#include <iostream>
#include <random>
#include <vector>

using std::chrono::duration;
using std::chrono::high_resolution_clock;

int total_keys = 1024;
std::vector<std::string> server_addresses;
std::vector<std::string> keys;
std::random_device rd;
std::mt19937 gen(rd());
std::uniform_int_distribution<> key_distrib(0, total_keys - 1);
std::uniform_int_distribution<> key_len_distrib(1, 128);
std::uniform_int_distribution<> value_len_distrib(1, 2048);
static const std::string characters =
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

std::string generate_random_string(size_t length, std::mt19937 &gen) {
  std::uniform_int_distribution<> distrib(0, characters.size() - 1);
  std::string random_string;
  random_string.reserve(length);
  for (size_t i = 0; i < length; ++i) {
    random_string += characters[distrib(gen)];
  }
  return random_string;
}

int send_reqs(int req_type) {
  char old_value[2049];
  char value[2049];

  int num_requests = 1000;
  int failed_requests = 0;

  for (int i = 0; i < num_requests; i++) {
    int key_index = key_distrib(gen);
    if (req_type) {
      std::string new_value =
          generate_random_string(value_len_distrib(gen), gen);

      int put_result =
          kv739_put(const_cast<char *>(keys[key_index].c_str()),
                    const_cast<char *>(new_value.c_str()), old_value);
      if (put_result < 0) {
        failed_requests += 1;
      }
    } else {
      int get_result =
          kv739_get(const_cast<char *>(keys[key_index].c_str()), value);
      if (get_result < 0) {
        failed_requests += 1;
      }
    }
  }
  double fail_rate = (failed_requests * 1.0 / num_requests);
  std::cout << "Failure Rate = " << fail_rate << std::endl;
  if (fail_rate >= 0.5) {
    return server_addresses.size();
  }
  return 0;
}

void run_availability_test(char *config_file, int total_servers) {
  assert(kv739_init(config_file) == 0);
  int get_limit = 0;
  int put_limit = 0;

  for (int i = 0; i < total_servers; i++) {
    std::cout << "Started Sending Requests for server_size = "
              << server_addresses.size() << std::endl;
    if (!put_limit) {
      put_limit = send_reqs(1);
    }
    if (!get_limit) {
      get_limit = send_reqs(0);
    }
    std::cout << "Completed Sending Requests for server_size = "
              << server_addresses.size() << std::endl;
    std::uniform_int_distribution<> server_choice(0,
                                                  server_addresses.size() - 1);
    int server_to_kill = server_choice(gen);
    kv739_leave(server_addresses[server_to_kill].data(), 0);
    server_addresses.erase(server_addresses.begin() + server_to_kill);
  }

  std::cout << "Availability server critical limit for GET, PUT = " << get_limit
            << ", " << put_limit << std::endl;

  assert(kv739_shutdown() == 0);
}

void initializeServersAndKeysList(int total_servers) {
  for (int port = 50051; port < 50051 + total_servers; port++) {
    server_addresses.push_back(std::string("127.0.0.1:") +
                               std::to_string(port));
  }

  for (int i = 0; i < total_keys; i++) {
    keys.push_back(generate_random_string(key_len_distrib(gen), gen));
  }
}

int main(int argc, char **argv) {
  std::string config_file = "20.config";
  int total_servers = 20;
  if (argc > 1) {
    total_servers = std::atoi(argv[1]);
    config_file = argv[2];
  }
  initializeServersAndKeysList(total_servers);
  run_availability_test(config_file.data(), total_servers);

  return 0;
}
