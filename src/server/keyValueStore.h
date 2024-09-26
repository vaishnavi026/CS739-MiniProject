#ifndef KEYVALUESTORE_H
#define KEYVALUESTORE_H

#include <mutex>
#include <sqlite3.h>
class keyValueStore {
private:
  sqlite3 *db;     // Database handle
  char *tablename; // Name of the table in DB (Storing key values persistently)
  std::mutex resource_mutex;
  std::mutex read_count_mutex;
  int reader_count = 0;

public:
  // Constructor and Destructor
  keyValueStore();
  ~keyValueStore();

  // Member functions to get and put key-value pairs
  int read(char *key, std::string &value);
  int write(char *key, char *value, std::string &old_value);
};

#endif // KEYVALUESTORE_H
