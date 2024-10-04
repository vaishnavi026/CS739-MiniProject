#include "client_interface.h"
#include <cstring>
#include <iostream>
#include <unistd.h> // For sleep

int main(int argc, char **argv) {

  char server_name[] = "0.0.0.0:50051";
  if (kv739_init(server_name) != 0) {
    return -1;
  }

  char key[] = "CS739";
  char value[] = "DurabilityTestValue";
  char get_value[2049] = {0};

  // Write a value to the key-value store
  std::cout << "Test 1: Writing a value before crash\n";
  char old_value[2049] = {0};
  int put_result = kv739_put(key, value, old_value);
  std::cout << put_result;

  if (put_result == 0) {
    std::cout << "PASS: Value written successfully.\n";
  } else {
    std::cout << "FAIL: Error writing value.\n";
  }

  // shutdown the server
  std::cout << "Simulating server crash (shutdown)\n";
  if (kv739_shutdown() != 0) {
    return -1;
  }

  sleep(5);

  // restart the server
  std::cout << "Restarting the server\n";
  if (kv739_init(server_name) != 0) {
    return -1;
  }

  // Step 4: Read the key after the server restart
  std::cout << "Test 2: Reading the value after server restart\n";
  int get_result = kv739_get(key, get_value);
  if (get_result == 0) {
    if (strcmp(value, get_value) == 0) {
      std::cout << "PASS: Value read matches the value written before crash: "
                << get_value << "\n";
    } else {
      std::cout << "FAIL: Value read does not match the value written before "
                   "crash: Expected "
                << value << ", got " << get_value << "\n";
    }
  } else {
    std::cout << "FAIL: Error reading value after restart.\n";
  }

  // Shutdown the server again
  if (kv739_shutdown() != 0) {
    return -1;
  }

  return 0;
}
