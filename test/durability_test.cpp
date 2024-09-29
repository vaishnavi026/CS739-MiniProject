#include "client_interface.h"
#include <iostream>
#include <unistd.h>
#include <signal.h>

int main(int argc, char **argv) {
    char server_name[] = "0.0.0.0:50051";
    if (kv739_init(server_name) != 0) {
        std::cerr << "Failed to initialize server connection.\n";
        return -1;
    }
    char key[] = "CS739";
    char value[] = "DurabilityTestValue";
    char get_value[2049] = {0};

    // Write a value to the key-value store
    std::cout << "Test 1: Writing a value before crash\n";
    char old_value[2049] = {0};
    int put_result = kv739_put(key, value, old_value);

    if (put_result == 1 || put_result == 0) {
        std::cout << "PASS: Value written successfully.\n";
    } else {
        std::cerr << "FAIL: Error writing value.\n";
        return -1;
    }

    if (system("pkill -SIGKILL server") != 0) {
        std::cerr << "Error killing the process for crash simulation.\n";
        return -1;
    }

    pid_t pid = fork();
    if (pid == 0) {
        execl("../src/server/server", "server", (char *)NULL);
        std::cerr << "Failed to start the server process.\n";
        return -1;
    } else if (pid > 0) {
        std::cout << "Server started successfully with PID: " << pid << "\n";
    } else {
        std::cerr << "Failed to fork the process.\n";
        return -1;
    }

    // Sleep for 15 secs to let the server start before sending requests.
    sleep(15);

    std::cout << "Restarting the client connection\n";
    if (kv739_init(server_name) != 0) {
        std::cerr << "Failed to reconnect to the server.\n";
        return -1;
    }

    // Read the key after server restart
    std::cout << "Test 2: Reading the value after server restart\n";
    int get_result = kv739_get(key, get_value);
    if (get_result == 0 || get_result == 1) {
    if (strcmp(value, get_value) == 0) {
        std::cout << "PASS: Value read matches the value written before crash: "
                << get_value << "\n";
    } else {
        std::cout << "FAIL: Value read does not match the value written before "
                    "crash: Expected "
                << value << ", got " << get_value << "\n";
    }
    } else {
        std::cerr << "FAIL: Error reading value after restart.\n";
    }

    // Clean up and shutdown
    if (kv739_shutdown() != 0) {
        std::cerr << "Failed to shut down client connection.\n";
        return -1;
    }

    return 0;
}