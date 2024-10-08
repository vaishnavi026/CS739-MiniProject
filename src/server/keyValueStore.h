#ifndef KEYVALUESTORE_H
#define KEYVALUESTORE_H

#include <mutex>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>

class keyValueStore {
private:
  rocksdb::DB *db;
  rocksdb::Options options;

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
