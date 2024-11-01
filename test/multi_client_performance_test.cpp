#include "kv739.h"
#include <cassert>
#include <chrono>
#include <cstring>
#include <iostream>
#include <map>
#include <memory>
#include <random>
#include <stdlib.h>
#include <string>
#include <thread>
#include <time.h>

using std::chrono::duration;
using std::chrono::high_resolution_clock;

const int key_value_range[] = {1, 10000};

void client_sim(int tid, int num_requests, double put_probability,
                float *latency_results, float *throughput_results) {
  high_resolution_clock::time_point start_time;
  high_resolution_clock::time_point end_time;

  std::random_device rd;
  std::mt19937 generator(rd());
  std::uniform_int_distribution<> distribution(key_value_range[0],
                                               key_value_range[1]);
  float *request_times = new float[num_requests];
  char key[256], value[256], old_value[256];

  for (int i = 0; i < num_requests; i++) {
    snprintf(key, sizeof(key), "%d", distribution(generator));
    double random_value = ((double)rand()) / RAND_MAX;
    if (random_value < put_probability) {
      snprintf(value, sizeof(value), "%d", distribution(generator));
      start_time = std::chrono::high_resolution_clock::now();
      kv739_put(key, value, old_value);
      end_time = std::chrono::high_resolution_clock::now();
    } else {
      start_time = std::chrono::high_resolution_clock::now();
      kv739_get(key, value);
      end_time = std::chrono::high_resolution_clock::now();
    }
    request_times[i] = std::chrono::duration_cast<std::chrono::microseconds>(
                           end_time - start_time)
                           .count();
  }

  std::sort(request_times, request_times + num_requests);
  int p99_idx = (int)(0.99 * num_requests);
  latency_results[tid] = request_times[p99_idx] / 1000.0;

  float total_time_ms = 0.0;
  for (int i = 0; i < num_requests; i++) {
    total_time_ms += request_times[i] / 1000.0;
  }

  throughput_results[tid] = (1.0 * num_requests) / total_time_ms;
  delete[] request_times;
}
const int num_requests_range[] = {2500, 7500};

void perform_benchmarking(int num_threads, double put_probability) {
  float *latency_results = new float[num_threads];
  float *throughput_results = new float[num_threads];
  std::vector<std::thread> all_threads;
  std::cout << num_threads << "," << (int)(100.0 * put_probability) << ",";

  std::random_device rd;
  std::mt19937 generator(rd());
  std::uniform_int_distribution<> distribution(num_requests_range[0],
                                               num_requests_range[1]);
  int num_requests = distribution(generator);

  for (int i = 0; i < num_threads; i++) {
    all_threads.emplace_back(client_sim, i, num_requests, put_probability,
                             latency_results, throughput_results);
  }

  for (auto &thread : all_threads) {
    thread.join();
  }

  float total_p99 = 0.0;
  float total_throughput = 0.0;
  for (int i = 0; i < num_threads; i++) {
    total_p99 += latency_results[i];
    total_throughput += throughput_results[i];
  }

  std::cout << total_p99 / num_threads << "," << total_throughput / num_threads
            << std::endl;

  delete[] latency_results;
  delete[] throughput_results;
}

int main(int argc, char **argv) {
  srand(time(NULL));
  std::string config_file = "20.config";
  int num_threads = std::atoi(argv[1]);

  assert(kv739_init(const_cast<char *>(config_file.c_str())) == 0);

  std::cout << "Num Threads, Percentage Puts, Avg P99 latency (ms), "
               "Avg Throughput (requests/ms)"
            << std::endl;
  for (double put_probability = 0.05; put_probability < 0.99;
       put_probability += 0.15) {
    perform_benchmarking(num_threads, put_probability);
  }

  assert(kv739_shutdown() == 0);
  return 0;
}