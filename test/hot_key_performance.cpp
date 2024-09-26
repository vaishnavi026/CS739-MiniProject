#include "client.h"
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

void hot_key_reqs(char *server_name, perf_metrics *metrics, int num_requests) {
  char old_value[2049];
  char value[2049];
  srand(time(NULL));
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> key_len_distrib(1, 128);
  std::uniform_int_distribution<> value_len_distrib(1, 2048);

  assert(kv739_init(server_name) == 0);

  int total_keys = 1024;
  int hot_key_count = total_keys * 0.1;
  std::vector<std::string> keys;

  std::uniform_int_distribution<> hot_key_distrib(0, hot_key_count - 1);
  std::uniform_int_distribution<> cold_key_distrib(hot_key_count,
                                                   total_keys - 1);
  std::uniform_real_distribution<> op_distrib(0, 1.0);
  std::uniform_real_distribution<> key_distrib(0, 1.0);

  for (int i = 0; i < total_keys; i++) {
    keys.push_back(generate_random_string(key_len_distrib(gen), gen));
    std::string new_value = generate_random_string(value_len_distrib(gen), gen);
    kv739_put(const_cast<char *>(keys[i].c_str()),
              const_cast<char *>(new_value.c_str()), old_value);
  }

#pragma omp parallel for num_threads(8)
  for (int i = 0; i < num_requests; i++) {
    double op_type = op_distrib(gen);
    double key_type = key_distrib(gen);
    int key_index;

    if (key_type <= 0.9) {
      key_index = hot_key_distrib(gen);
    } else {
      key_index = cold_key_distrib(gen);
    }

    if (op_type > 0.9) {
      std::string new_value =
          generate_random_string(value_len_distrib(gen), gen);
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

void run_performance_test(char *server_name, int num_requests) {
  perf_metrics metrics;

  hot_key_reqs(server_name, &metrics, num_requests);

  double throughput = metrics.total_requests / (metrics.total_latency_ns / 1e9);
  double average_latency_ms =
      (metrics.total_latency_ns / 1e6) / metrics.total_requests;

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
  std::string server_address("0.0.0.0:50051");
  int num_requests = 100000;
  if (argc > 1) {
    num_requests = std::atoi(argv[1]);
    if (argc == 3)
      server_address = argv[2];
  }
  run_performance_test(server_address.data(), num_requests);

  return 0;
}
