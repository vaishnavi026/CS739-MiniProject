#include "client_interface.h"
#include <cstring>
#include <iostream>

int main(int argc, char **argv) {

  std::string server_name = "127.0.0.1:50051";
  std::string write_key = "";
  std::string write_value = "";
  std::string read_key = "";
  std::string expected_read_value = "";
  int client_number, get_result, put_result, read_or_write;

  if (argc > 1) {
    server_name = argv[1];
    client_number = std::atoi(argv[2]);
    read_or_write = std::atoi(argv[3]); // 1 for write , 0 for read
    if (read_or_write == 1) {           // Write condition
      write_key = argv[4];
      write_value = argv[5];
    } else {
      read_key = argv[4];
      expected_read_value = argv[5];
    }
  }

  if (kv739_init((char *)server_name.data()) != 0) {
    return -1;
  }

  if (read_or_write == 1) // Write condition
  {
    char old_value[2049] = {0};
    put_result = kv739_put((char *)write_key.data(), (char *)write_value.data(),
                           old_value);
    if (put_result != -1) {
      std::cout << "PASS : Wrote value = " << write_value
                << " at key = " << write_key
                << " for client = " << client_number << "\n";
    } else {
      std::cout << "FAIL : Error occured, the read was not succesful\n";
    }
  } else {
    char get_value[2049] = {0};
    get_result = kv739_get((char *)read_key.data(), get_value);
    if (get_result == 0) {
      if (strcmp(get_value, expected_read_value.data()) == 0) {
        std::cout << "PASS : Read value = " << get_value
                  << " matched the expected value for client = "
                  << client_number << "\n";
      } else {
        std::cout << "FAIL : Read value = " << get_value
                  << " did not match the expected value for client = "
                  << client_number << "\n";
      }
    } else if (get_result == 1) {
      std::cout << "FAIL : No value exists for the given key = " << read_key
                << " in the key value store\n";
    } else {
      std::cout << "FAIL : Error occured, the read was not succesful\n";
    }
  }

  if (kv739_shutdown() != 0) {
    return -1;
  }

  return 0;
}