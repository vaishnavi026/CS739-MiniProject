#ifndef KEYVALUESTORE_H
#define KEYVALUESTORE_H

#include <sqlite3.h>
#include <mutex>
class keyValueStore
{
private:
    sqlite3 *db;        // Database handle
    char* tablename;    // Name of the table in DB (Storing key values persistently)
    std::mutex m;
    std::condition_variable c;
    int reader_count = 0;
    bool is_writing = false;


public:
    // Constructor and Destructor
    keyValueStore();
    ~keyValueStore();

    // Member functions to get and put key-value pairs
    int get(char *key, char *value);
    char* put(char *key, char *value);
};

#endif // KEYVALUESTORE_H
