#ifndef KEYVALUESTORE_H
#define KEYVALUESTORE_H

#include <mutex>
#include <sqlite3.h>
class keyValueStore {
private:
  sqlite3 *shards[8];
  std::mutex db_mutexes[8];
  std::hash<std::string> hash_fn;
  // std::mutex read_count_mutex;
  // int reader_count = 0;

  int readdb(sqlite3 *db, const char *key, std::string &value);
  int writedb(sqlite3 *db, const char *key, const char *value,
              std::string &old_value);
  void opendb(const std::string &db_name, sqlite3 **db);
  void execdb(sqlite3 **db, const char *sql);

public:
  // Constructor and Destructor
  keyValueStore();
  ~keyValueStore();

  // Member functions to get and put key-value pairs
  int read(const std::string &key, std::string &value);
  int write(const std::string &key, const std::string &value,
            std::string &old_value);
};

#endif // KEYVALUESTORE_H
