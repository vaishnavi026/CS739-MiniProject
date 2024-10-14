#include "keyValueStore.h"
#include <iostream>
#include <mutex>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

keyValueStore::keyValueStore(std::string &server_address) {
  options.create_if_missing = true;
  std::string db_name = "rocksdb_" + server_address;
  rocksdb::Status status = rocksdb::DB::Open(options, db_name, &db);
  if (!status.ok()) {
    std::cerr << "Error opening database: " << status.ToString() << std::endl;
    exit(1);
  }
}

keyValueStore::~keyValueStore() { delete db; }

bool keyValueStore::parseValue(const std::string combined_value,
                               uint64_t &timestamp, std::string &value) {
  // Get delimited position
  size_t delimiter_pos = combined_value.find('|');
  // std::cout << "Combined value " << combined_value << "  " <<
  // std::to_string(delimiter_pos) << std::endl;
  if (delimiter_pos != std::string::npos && delimiter_pos > 0 &&
      delimiter_pos < combined_value.size() - 1) {
    timestamp =
        std::stoull(combined_value.substr(0, delimiter_pos)); // get usigned_int
    value = combined_value.substr(delimiter_pos +
                                  1); // Get the value after the delimiter
    return true;
  }

  std::cerr << "Error: Invalid format for combined value" << std::endl;
  return false;
}

int keyValueStore::read(const std::string &key, std::string &value) {
  rocksdb::Status status;

  status = db->Get(rocksdb::ReadOptions(), key, &value);

  if (status.ok()) {
    return 0;
  } else if (status.IsNotFound()) {
    return 1;
  }

  std::cerr << "Error getting value: " << status.ToString() << std::endl;
  return -1;
}

int keyValueStore::write(const std::string &key, const std::string &value,
                         uint64_t timestamp, std::string &old_value) {
  rocksdb::Status get_status;
  rocksdb::Status put_status;
  std::cout << "Rocksdb write for key, value, timestamp " << key << ", "
            << value << ", " << timestamp << std::endl;
  get_status = db->Get(rocksdb::ReadOptions(), key, &old_value);
  // std::cout << "Old value is " << old_value << std::endl;
  std::string new_value = std::to_string(timestamp) + "|" + value;
  put_status = db->Put(rocksdb::WriteOptions(), key, new_value);

  if (!put_status.ok()) {
    std::cerr << "Error putting value: " << put_status.ToString() << std::endl;
    return -1;
  }

  if (!get_status.ok()) {
    return 1;
  }

  return 0;
}

std::vector<std::pair<std::string, std::string>>
keyValueStore::getAllLatestKeys(uint64_t server_timestamp) {
  rocksdb::Iterator *it = db->NewIterator(rocksdb::ReadOptions());
  std::vector<std::pair<std::string, std::string>> latest_keys;

  if (!it->status().ok()) {
    std::cerr << "Error during iteration: " << it->status().ToString()
              << std::endl;
    return latest_keys;
  }

  // Iterate over the key-value pairs
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    std::string key = it->key().ToString();
    std::string value = it->value().ToString();

    std::string old_value;
    uint64_t timestamp;
    parseValue(value, timestamp, old_value);

    if (server_timestamp < timestamp) {
      latest_keys.push_back({key, value});
    }
  }
  return latest_keys;
}

void ConsistentHashing::addServer(const std::string &server_name) {
  for (int i = 0; i < virtualServers_; ++i) {
    std::string virtualServerName = server_name + "-" + std::to_string(i);
    std::size_t hash = hash_fn(virtualServerName);
    ring_[hash] = server_name;
  }
}

void ConsistentHashing::removeServer(const std::string &server_name) {
  for (int i = 0; i < virtualServers_; ++i) {
    std::string virtualServerName = server_name + "-" + std::to_string(i);
    std::size_t hash = hash_fn(virtualServerName);
    ring_.erase(hash);
  }
}

std::string ConsistentHashing::getServer(const std::string &key) {

  if (ring_.empty()) {
    std::cerr << "No consisent hashing ring" << std::endl;
    exit(1);
  }

  std::size_t hash = hash_fn(key);
  auto it = ring_.lower_bound(hash);

  if (it == ring_.end()) {
    it = ring_.begin();
  }

  return it->second;
}
