#include "client_interface.h"
#include <iostream>

int main(int argc, char **argv) {

  char server_name[] = "0.0.0.0:50051";
  if (kv739_init(server_name) != 0) {
    return -1;
  }

  char key[] = "CS739";
  char value[] = "Distributed Systems, MIKE SWIFT";
  char update_value[] = "Distributed Systems, MIKE SWIFT, FALL 2024";

  //Case 1: Reading value not present 
  std::cout << "Test 1 : Reading value not present\n"; 

  char get_value[2049] = {0};
  int get_result, put_result;
  get_result = kv739_get(key, get_value);
  if (get_result == 0) {
    std::cout << "FAIL : Reading value not present : Expected response 1, observed 0\n";
  }else if(get_result == 1){
    std::cout << "PASS : Reading value not present : Expected response 1, observed 1\n";
  }else if(get_result == -1){
    std::cout << "FAIL : DB failed not expected\n";
  }else{
    std::cout << "FAIL : Unknown return code\n";
  }

  //Case 2: Writing old value not present
  std::cout << "Test 2 : Writing old value not present\n"; 

  char old_value[2049] = {0};
  put_result = kv739_put(key, value, old_value);
  if (put_result == 0) {
    std::cout << "FAIL : Writing old value not present : Expected response 1, observed 0\n";
  }else if (put_result == 1) {
    std::cout << "PASS : Writing old value not present : Expected response 1, observed 1\n";
  }else if (put_result == -1){
    std::cout << "FAIL : DB failed not expected\n";
  }else{
    std::cout << "FAIL : Unknown return code\n";
  }

  //Case 3: Reading value present
  std::cout << "Test 3 : Reading value present\n";

  char get_value2[2049] = {0};
  get_result = kv739_get(key, get_value2);
  if (get_result == 0) {
    if(strcmp(value,get_value2) == 0)
        std::cout << "PASS : Reading value present : Expected response 0, observed 0 : Expected read value = " << value << ", Observed read value = " << get_value2 << "\n";
    else
        std::cout << "FAIL : Reading value present : Expected response 0, observed 0 : Expected read value = " << value << ", Observed read value = " << get_value2 << "\n";
  }else if(get_result == 1){
    std::cout << "FAIL : Reading value present : Expected response 0, observed 1\n";
  }else if(get_result == -1){
    std::cout << "FAIL : DB failed not expected\n";
  }else{
    std::cout << "FAIL : Unknown return code\n";
  }

  //Case 4 : Writing with old value present 
  std::cout << "Test 4 : Writing with old value present\n";

  char old_value2[2049] = {0};
  put_result = kv739_put(key, update_value, old_value2);
  if (put_result == 0) {
    if(strcmp(old_value2,value) == 0)
        std::cout << "PASS : Writing with old value present : Expected response 0, observed 0 : Expected old value = " << value << ", Observed old value = " << old_value2 << "\n";
    else
        std::cout << "FAIL : Writing with old value present : Expected response 0, observed 0 : Expected old value = " << value << ", Observed old value = " << old_value2 << "\n";
  }else if (put_result == 1) {
    std::cout << "FAIL : Writing with old value present : Expected response 0, observed 1\n";
  }else if (put_result == -1){
    std::cout << "FAIL : DB failed not expected\n";
  }else{
    std::cout << "FAIL : Unknown return code\n";
  }

  //Case 5: Verifying updated write value  
  std::cout << "Test 5 : Verifying updated write value\n";

  char get_value3[2049] = {0};
  get_result = kv739_get(key, get_value3);
  if (get_result == 0) {
    if(strcmp(update_value,get_value3) == 0)
        std::cout << "PASS : Reading value present : Expected response 0, observed 0 : Expected read value = " << update_value << ", Observed read value = " << get_value3 << "\n";
    else
        std::cout << "FAIL : Reading value present : Expected response 0, observed 0 : Expected read value = " << update_value << ", Observed read value = " << get_value3 << "\n";
  }else if(get_result == 1){
    std::cout << "FAIL : Reading value present : Expected response 0, observed 1\n";
  }else if(get_result == -1){
    std::cout << "FAIL : DB failed not expected\n";
  }else{
    std::cout << "FAIL : Unknown return code\n";
  }

  if (kv739_shutdown() != 0) {
    return -1;
  }

  return 0;
} 