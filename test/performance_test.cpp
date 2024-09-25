#include "client.h"
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstring>
#include <ctime>
#include <iostream>
#include <omp.h>
#include <vector>

using std::chrono::duration;
using std::chrono::high_resolution_clock;

struct perf_metrics {
  std::atomic<int> total_requests{0};
  std::atomic<int> successful_requests{0};
  std::atomic<long long> total_latency_ns{0};
};

void client(char *server_name, perf_metrics *metrics, int num_requests) {
  char old_value[2049];
  char value[2049];

  assert(kv739_init(server_name) == 0);
  srand(time(NULL));

  // #pragma omp parallel for
  for (int i = 0; i < num_requests; i++) {
    std::string key = "key_" + std::to_string(i % 10);
    std::string new_value = "value_" + std::to_string(i % 10);

    if (rand() % 2 == 0) {
      high_resolution_clock::time_point start_time =
          high_resolution_clock::now();
      int put_result =
          kv739_put(const_cast<char *>(key.c_str()),
                    const_cast<char *>(new_value.c_str()), old_value);
      high_resolution_clock::time_point end_time = high_resolution_clock::now();

      duration<long long, std::nano> duration_nano =
          std::chrono::duration_cast<duration<long long, std::nano>>(
              end_time - start_time);
      long long latency_ns = duration_nano.count();

      metrics->total_latency_ns += latency_ns;

      if (put_result == 0 || put_result == 1) {
        metrics->successful_requests++;
      }
    } else {
      high_resolution_clock::time_point start_time =
          high_resolution_clock::now();
      int get_result = kv739_get(const_cast<char *>(key.c_str()), value);
      high_resolution_clock::time_point end_time = high_resolution_clock::now();

      duration<long long, std::nano> duration_nano =
          std::chrono::duration_cast<duration<long long, std::nano>>(
              end_time - start_time);
      long long latency_ns = duration_nano.count();

      metrics->total_latency_ns += latency_ns;

      if (get_result == 0 || get_result == 1) {
        metrics->successful_requests++;
      }
    }
    metrics->total_requests++;
  }

  assert(kv739_shutdown() == 0);
}

void run_performance_test(char *server_name, int num_requests) {

  perf_metrics metrics;

  high_resolution_clock::time_point start_time = high_resolution_clock::now();

  client(server_name, &metrics, num_requests);

  high_resolution_clock::time_point end_time = high_resolution_clock::now();

  duration<long long, std::nano> total_duration_nano =
      std::chrono::duration_cast<duration<long long, std::nano>>(end_time -
                                                                 start_time);
  double test_duration_sec = total_duration_nano.count() / 1e9;

  double throughput = metrics.total_requests / test_duration_sec;
  double average_latency_ms =
      (metrics.total_latency_ns / 1e6) / metrics.total_requests;

  std::cout << "Total requests: " << metrics.total_requests << std::endl;
  std::cout << "Successful requests: " << metrics.successful_requests
            << std::endl;
  std::cout << "Failed requests: "
            << metrics.total_requests - metrics.successful_requests
            << std::endl;
  std::cout << "Total duration: " << test_duration_sec << " seconds"
            << std::endl;
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
