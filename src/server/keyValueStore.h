#ifndef KEYVALUESTORE_H
#define KEYVALUESTORE_H

#include <sqlite3.h>
#include <mutex>
class keyValueStore
{
private:
    sqlite3 *db;        // Database handle
    char* tablename;    // Name of the table in DB (Storing key values persistently)
    std::mutex resource_mutex;
    std::mutex read_count_mutex;
    int reader_count = 0;


public:
    // Constructor and Destructor
    keyValueStore();
    ~keyValueStore();

    // Member functions to get and put key-value pairs
    int read(char *key, char *value);
    char* write(char *key, char *value);
};

#endif // KEYVALUESTORE_H
