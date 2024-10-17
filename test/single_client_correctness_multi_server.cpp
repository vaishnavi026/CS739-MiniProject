#include "client_interface.h"
#include <cstring>
#include <iostream>
#include <unistd.h>

int main(int argc, char **argv) {

  std::string server_name = "127.0.0.1:50051";
  std::string config_file = "server_connection.txt";
  int put_result, get_result;

  if (argc > 1) {
    server_name = argv[1];
  }

  if (kv739_init((char *)config_file.data()) != 0) {
    return -1;
  }

  std::cout << "Coming to this point" << std::endl;
  // sleep(5000);

  char key[] = "CS739";
  char illegalkey[] = "CS739[]";
  char value[] = "DistributedSystemsMIKESWIFT";
  char illegalvalue[] = "DistributedSystemsMIKESWIFT[]";
  char update_value[] = "DistributedSystemsMIKESWIFTFALL2024";

  // Case 1: Reading value not present
  std::cout << "Test 1 : Reading value not present\n";

  char get_value[2049] = {0};

  get_result = kv739_get(key, get_value);
  if (get_result == 0) {
    std::cout << "FAIL : Reading value not present : Expected response 1, "
                 "observed 0\n";
  } else if (get_result == 1) {
    std::cout << "PASS : Reading value not present : Expected response 1, "
                 "observed 1\n";
  } else if (get_result == -1) {
    std::cout << "FAIL : DB Failure or Problem in Key or Client did not "
                 "receive packet due to network issues\n";
  } else {
    std::cout << "FAIL : Unknown return code\n";
  }

  // Case 2: Writing old value not present
  std::cout << "Test 2 : Writing old value not present\n";

  char old_value[2049] = {0};
  put_result = kv739_put(key, value, old_value);
  if (put_result == 0) {
    std::cout << "FAIL : Writing old value not present : Expected response 1, "
                 "observed 0\n";
  } else if (put_result == 1) {
    std::cout << "PASS : Writing old value not present : Expected response 1, "
                 "observed 1\n";
  } else if (put_result == -1) {
    std::cout << "FAIL : DB Failure or Problem in Key/Value or Client did not "
                 "receive packet due to network issues\n";
  } else {
    std::cout << "FAIL : Unknown return code\n";
  }

  // Case 3: Reading value present
  std::cout << "Test 3 : Reading value present\n";

  char get_value2[2049] = {0};
  get_result = kv739_get(key, get_value2);
  std::cout << "Returning get value2 " << get_value2 << std::endl;
  // if (get_result == 0) {
  //   if (strcmp(value, get_value2) == 0)
  //     std::cout << "PASS : Reading value present : Expected response 0, "
  //                  "observed 0 : Expected read value = "
  //               << value << ", Observed read value = " << get_value2 << "\n";
  //   else
  //     std::cout << "FAIL : Reading value present : Expected response 0, "
  //                  "observed 0 : Expected read value = "
  //               << value << ", Observed read value = " << get_value2 << "\n";
  // } else if (get_result == 1) {
  //   std::cout
  //       << "FAIL : Reading value present : Expected response 0, observed
  //       1\n";
  // } else if (get_result == -1) {
  //   std::cout << "FAIL : DB Failure or Problem in Key or Client did not "
  //                "receive packet due to network issues\n";
  // } else {
  //   std::cout << "FAIL : Unknown return code\n";
  // }

  // Case 4 : Writing with old value present
  std::cout << "Test 4 : Writing with old value present\n";

  char old_value2[2049] = {0};
  put_result = kv739_put(key, update_value, old_value2);
  if (put_result == 0) {
    if (strcmp(old_value2, value) == 0)
      std::cout << "PASS : Writing with old value present : Expected response "
                   "0, observed 0 : Expected old value = "
                << value << ", Observed old value = " << old_value2 << "\n";
    else
      std::cout << "FAIL : Writing with old value present : Expected response "
                   "0, observed 0 : Expected old value = "
                << value << ", Observed old value = " << old_value2 << "\n";
  } else if (put_result == 1) {
    std::cout << "FAIL : Writing with old value present : Expected response 0, "
                 "observed 1\n";
  } else if (put_result == -1) {
    std::cout << "FAIL : DB Failure or Problem in Key/Value or Client did not "
                 "receive packet due to network issues\n";
  } else {
    std::cout << "FAIL : Unknown return code\n";
  }

  // Case 5: Verifying updated write value
  std::cout << "Test 5 : Verifying updated write value\n";

  char get_value3[2049] = {0};
  get_result = kv739_get(key, get_value3);
  if (get_result == 0) {
    if (strcmp(update_value, get_value3) == 0)
      std::cout << "PASS : Reading value present : Expected response 0, "
                   "observed 0 : Expected read value = "
                << update_value << ", Observed read value = " << get_value3
                << "\n";
    else
      std::cout << "FAIL : Reading value present : Expected response 0, "
                   "observed 0 : Expected read value = "
                << update_value << ", Observed read value = " << get_value3
                << "\n";
  } else if (get_result == 1) {
    std::cout
        << "FAIL : Reading value present : Expected response 0, observed 1\n";
  } else if (get_result == -1) {
    std::cout << "FAIL : DB Failure or Problem in Key or Client did not "
                 "receive packet due to network issues\n";
  } else {
    std::cout << "FAIL : Unknown return code\n";
  }

  // Case 6 : Problem in Key only(Not meeting the required format) during get
  std::cout << "Test 6 : Problem in Read : Key not meeting the required format "
               "during get\n";
  char get_value4[2049] = {0};
  get_result = kv739_get(illegalkey, get_value4);
  if (get_result == 0) {
    std::cout << "FAIL : Reading value not present : Expected response 1, "
                 "observed 0\n";
  } else if (get_result == 1) {
    std::cout << "FAIL : Reading value not present : Expected response 1, "
                 "observed 1\n";
  } else if (get_result == -1) {
    std::cout << "PASS : DB Failure or Problem in Key or Client did not "
                 "receive packet due to network issues\n";
  } else {
    std::cout << "FAIL : Unknown return code\n";
  }

  // Case 7 : Problem in Key only(Not meeting the required format) during put
  char old_value3[2049] = {0};
  std::cout << "Test 7 : Problem in Key only(Not meeting the required format) "
               "during put\n";

  put_result = kv739_put(illegalkey, value, old_value3);

  if (put_result == 0) {
    std::cout << "FAIL : Writing old value not present : Expected response 1, "
                 "observed 0\n";
  } else if (put_result == 1) {
    std::cout << "FAIL : Writing old value not present : Expected response 1, "
                 "observed 1\n";
  } else if (put_result == -1) {
    std::cout << "PASS : DB Failure or Problem in Key/Value or Client did not "
                 "receive packet due to network issues\n";
  } else {
    std::cout << "FAIL : Unknown return code\n";
  }

  // Case 8 : Problem in Value only(Not meeting the required format) during put
  std::cout << "Test 8 : Problem in Value only(Not meeting the required "
               "format) during put\n";
  char old_value4[2049] = {0};

  put_result = kv739_put(key, illegalvalue, old_value4);
  std::cout << "Test 7 : Problem in Key only(Not meeting the required format) "
               "during put\n";
  if (put_result == 0) {
    std::cout << "FAIL : Writing old value not present : Expected response 1, "
                 "observed 0\n";
  } else if (put_result == 1) {
    std::cout << "FAIL : Writing old value not present : Expected response 1, "
                 "observed 1\n";
  } else if (put_result == -1) {
    std::cout << "PASS : DB Failure or Problem in Key/Value or Client did not "
                 "receive packet due to network issues\n";
  } else {
    std::cout << "FAIL : Unknown return code\n";
  }

  // Case 9 : Problem in Value and Key both(Not meeting the required format)
  // during put
  std::cout << "Test 9 : Problem in Key and Value both(Not meeting the "
               "required format) during put\n";
  char old_value5[2049] = {0};

  put_result = kv739_put(illegalkey, illegalvalue, old_value5);
  std::cout << "Test 7 : Problem in Key only(Not meeting the required format) "
               "during put\n";
  if (put_result == 0) {
    std::cout << "FAIL : Writing old value not present : Expected response 1, "
                 "observed 0\n";
  } else if (put_result == 1) {
    std::cout << "FAIL : Writing old value not present : Expected response 1, "
                 "observed 1\n";
  } else if (put_result == -1) {
    std::cout << "PASS : DB Failure or Problem in Key/Value or Client did not "
                 "receive packet due to network issues\n";
  } else {
    std::cout << "FAIL : Unknown return code\n";
  }

  if (kv739_shutdown() != 0) {
    return -1;
  }

  return 0;
}