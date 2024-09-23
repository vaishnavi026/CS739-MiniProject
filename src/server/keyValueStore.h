#ifndef KEYVALUESTORE_H
#define KEYVALUESTORE_H

#include <sqlite3.h>

class keyValueStore
{
private:
    sqlite3 *db;        // Database handle
    char* tablename;    // Name of the table in DB (Storing key values persistently)

public:
    // Constructor and Destructor
    keyValueStore();
    ~keyValueStore();

    // Member functions to get and put key-value pairs
    int get(char *key, char *value);
    int put(char *key, char *value, char *old_value);
};

#endif // KEYVALUESTORE_H
