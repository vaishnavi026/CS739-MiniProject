#include "client_interface.h"
#include <cstring> // For memset
#include <iostream>
#include <thread> // For multithreading

// Function for getting the value from the key-value store in a separate thread
void get_value_from_store(char *key, char *result_buffer) {
  int get_result2 = kv739_get(key, result_buffer);
  if (get_result2 == 0) {
    std::cout << "Key Found, Value is " << result_buffer << "\n";
  } else if (get_result2 == 1) {
    std::cout << "Key Not Found!\n";
  }
}

int main(int argc, char **argv) {
  char server_name[] = "127.0.0.1:50051";
  if (kv739_init(server_name) != 0) {
    return -1;
  }

  char key[] = "CS7391";
  char value[] = "DurabilityTestValue1";
  char old_value[2049] = {0};

  // Write a value to the key-value store
  std::cout << "Test 1: Writing a value before crash\n";
  int put_result = kv739_put(key, value, old_value);

  if (put_result == 0 || put_result == 1) {
    std::cout << "PASS: Value written successfully.\n";
  } else {
    std::cout << "FAIL: Error writing value.\n";
    return -1;
  }

  // Prepare buffers for storing the value retrieved by each thread
  char get_value1[2049] = {0};
  char get_value2[2049] = {0};

  // Launch two threads to read the value in parallel
  std::thread t1(get_value_from_store, key, get_value1);
  std::thread t2(get_value_from_store, key, get_value2);

  // Wait for both threads to finish
  t1.join();
  t2.join();

  return 0;
}
