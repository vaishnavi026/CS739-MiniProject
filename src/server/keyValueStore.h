#ifndef KEYVALUESTORE_H
#define KEYVALUESTORE_H

#include "dbConnectionPool.h"
#include <mutex>
#include <sqlite3.h>

class keyValueStore {
private:
  // char *tablename;
  std::mutex resource_mutex;
  std::mutex read_count_mutex;
  int reader_count = 0;
  DBConnectionPool *db_pool;
  std::string tablename;

public:
  // Constructor and Destructor
  keyValueStore(size_t pool_size = 8);
  ~keyValueStore();

  // Member functions to get and put key-value pairs
  int read(char *key, std::string &value);
  int write(char *key, char *value, std::string &old_value);
};

#endif // KEYVALUESTORE_H
