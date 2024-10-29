#ifndef KEYVALUESTORE_H
#define KEYVALUESTORE_H

#include <mutex>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>

class ConsistentHashing;
class keyValueStore;

class ConsistentHashing {
private:
  int virtualServers_;
  std::map<std::size_t, std::string> ring_;
  std::hash<std::string> hash_fn;

public:
  ConsistentHashing(int virtualServers) : virtualServers_(virtualServers) {};
  void addServer(const std::string &server_name);
  void removeServer(const std::string &server_name);
  std::string getServer(const std::string &key);
  void printServersRing();
};

class keyValueStore {
private:
  rocksdb::DB *db;
  rocksdb::Options options;

public:
  // Constructor and Destructor
  keyValueStore(std::string &server_address);
  ~keyValueStore();

  // Member functions to get and put key-value pairs
  int read(const std::string &key, std::string &value);
  int write(const std::string &key, const std::string &value,
            std::uint64_t timestamp, std::string &old_value);
  int batched_write(
      std::vector<std::pair<std::string, std::string>> &key_value_batch);
  // Get all keys in RocksDB
  std::vector<std::pair<std::string, std::string>>
  getAllLatestKeys(std::uint64_t timestamp);

  bool parseValue(const std::string combined_value, uint64_t &timestamp,
                  std::string &value);
};

#endif // KEYVALUESTORE_H
