#include "client.h"
#include <iostream>

int main(int argc, char **argv) {
  char server_name[] = "0.0.0.0:50051";
  if (kv739_init(server_name) != 0) {
    return -1;
  }

  // Expected outputs: With Fresh DB
  // Key Not Found!
  // Put Success, no old value
  // Put Success, old value Distributed Systems, MIKE SWIFT
  // Key Found, Value is Distributed Systems, MIKE SWIFT
  // Put Success, old value Distributed Systems, MIKE SWIFT
  // Key Found, Value is Distributed Systems, MIKE SWIFT, FALL 2024

  char key[] = "CS739";
  char value[] = "Distributed Systems, MIKE SWIFT";
  char update_value[] = "Distributed Systems, MIKE SWIFT, FALL 2024";

  char get_value[2049] = {0};
  int get_result = kv739_get(key, get_value);
  if (get_result == 0) {
    std::cout << "Key Found, Value is " << get_value << "\n";
  } else if (get_result == 1) {
    std::cout << "Key Not Found!\n";
  }

  char old_value[2049] = {0};
  int put_result = kv739_put(key, value, old_value);
  if (put_result == 1) {
    std::cout << "Put Success, no old value\n";
  } else if (put_result == 0) {
    std::cout << "Put Success, old value " << old_value << "\n";
  }

  int put_result2 = kv739_put(key, value, old_value);
  if (put_result2 == 1) {
    std::cout << "Put Success, no old value\n";
  } else if (put_result2 == 0) {
    std::cout << "Put Success, old value " << old_value << "\n";
  }

  int get_result2 = kv739_get(key, get_value);
  if (get_result2 == 0) {
    std::cout << "Key Found, Value is " << get_value << "\n";
  } else if (get_result2 == 1) {
    std::cout << "Key Not Found!\n";
  }

  char old_value2[2049] = {0};
  int put_result3 = kv739_put(key, update_value, old_value2);
  if (put_result3 == 1) {
    std::cout << "Put Success, no old value\n";
  } else if (put_result3 == 0) {
    std::cout << "Put Success, old value " << old_value2 << "\n";
  }

  char get_value2[2049] = {0};
  int get_result3 = kv739_get(key, get_value2);
  if (get_result3 == 0) {
    std::cout << "Key Found, Value is " << get_value2 << "\n";
  } else if (get_result3 == 1) {
    std::cout << "Key Not Found!\n";
  }

  if (kv739_shutdown() != 0) {
    return -1;
  }

  return 0;
}