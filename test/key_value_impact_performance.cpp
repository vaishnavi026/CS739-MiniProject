#include "client_interface.h"
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstring>
#include <ctime>
#include <iostream>
#include <omp.h>
#include <random>
#include <vector>

using std::chrono::duration;
using std::chrono::high_resolution_clock;

struct perf_metrics {
  std::atomic<int> total_requests{0};
  std::atomic<int> read_requests{0};
  std::atomic<int> write_requests{0};
  std::atomic<int> successful_requests{0};
  std::atomic<long long> total_latency_ns{0};
};
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

void gen_reqs(char *server_name, perf_metrics *metrics, int num_requests,
              int value_sz) {
  char old_value[2049];
  char value[2049];
  srand(time(NULL));
  std::random_device rd;
  std::mt19937 gen(rd());
  int key_sz = 64;

  if (kv739_init(server_name) != 0) {
    return;
  }

  int total_keys = 1024;
  std::vector<std::string> keys;
  std::uniform_real_distribution<> op_distrib(0, 1.0);
  std::uniform_int_distribution<> key_dist(0, total_keys - 1);
  int key_index = key_dist(gen);
  for (int i = 0; i < total_keys; i++) {
    keys.push_back(generate_random_string(key_sz, gen));
    std::string new_value = generate_random_string(value_sz, gen);
    kv739_put(const_cast<char *>(keys[i].c_str()),
              const_cast<char *>(new_value.c_str()), old_value);
  }
// std::cout << num_requests << std::endl;
#pragma omp parallel for num_threads(8)
  for (int i = 0; i < num_requests; i++) {
    double op_type = op_distrib(gen);

    if (op_type > 0.5) {
      std::string new_value = generate_random_string(value_sz, gen);
      high_resolution_clock::time_point start_time =
          high_resolution_clock::now();
      int put_result =
          kv739_put(const_cast<char *>(keys[key_index].c_str()),
                    const_cast<char *>(new_value.c_str()), old_value);
      high_resolution_clock::time_point end_time = high_resolution_clock::now();

      duration<long long, std::nano> duration_nano =
          std::chrono::duration_cast<duration<long long, std::nano>>(
              end_time - start_time);
      long long latency_ns = duration_nano.count();

      metrics->total_latency_ns += latency_ns;

      if (put_result == 0 || put_result == 1) {
        metrics->successful_requests++;
        metrics->write_requests++;
      }
    } else {
      high_resolution_clock::time_point start_time =
          high_resolution_clock::now();
      int get_result =
          kv739_get(const_cast<char *>(keys[key_index].c_str()), value);
      high_resolution_clock::time_point end_time = high_resolution_clock::now();

      duration<long long, std::nano> duration_nano =
          std::chrono::duration_cast<duration<long long, std::nano>>(
              end_time - start_time);
      long long latency_ns = duration_nano.count();

      metrics->total_latency_ns += latency_ns;

      if (get_result == 0 || get_result == 1) {
        metrics->successful_requests++;
        metrics->read_requests++;
      }
    }
    metrics->total_requests++;
  }

  assert(kv739_shutdown() == 0);
}

void run_performance_test(char *server_name, int num_requests, int value_len) {
  perf_metrics metrics;

  gen_reqs(server_name, &metrics, num_requests, value_len);

  double throughput = metrics.total_requests / (metrics.total_latency_ns / 1e9);
  double average_latency_ms =
      (metrics.total_latency_ns / 1e6) / metrics.total_requests;
  std::cout << "Value Len: " << value_len << std::endl;
  std::cout << "Total requests: " << metrics.total_requests << std::endl;
  std::cout << "Successful requests: " << metrics.successful_requests
            << std::endl;
  std::cout << "Failed requests: "
            << metrics.total_requests - metrics.successful_requests
            << std::endl;
  std::cout << "Read requests: " << metrics.read_requests << std::endl;
  std::cout << "Write requests: " << metrics.write_requests << std::endl;
  std::cout << "Throughput: " << throughput << " rps" << std::endl;
  std::cout << "Average latency: " << average_latency_ms << " ms" << std::endl;
}

int main(int argc, char **argv) {
  std::string config_file = "32.config";
  int num_requests = 50000;
  int value_len = 1024;
  if (argc > 1) {
    num_requests = std::atoi(argv[1]);
    if (argc >= 3) {
      value_len = std::atoi(argv[2]);
    }
    if (argc == 4) {
      config_file = argv[3];
    }
  }
  // for(int i= 0;i<12;i++)
    run_performance_test(config_file.data(), num_requests, value_len);

  return 0;
}
