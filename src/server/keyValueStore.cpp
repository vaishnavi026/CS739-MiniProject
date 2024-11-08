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
  std::string db_name = "db_" + server_address;
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
  rocksdb::WriteBatch batch;
  std::string get_value_timestamp;

  get_status = db->Get(rocksdb::ReadOptions(), key, &get_value_timestamp);

  if (get_status.ok()) {
    uint64_t get_timestamp;
    std::string get_value;
    parseValue(get_value_timestamp, get_timestamp, get_value);

    if (get_timestamp > timestamp) {
      old_value = std::to_string(timestamp) + "|" + value;
      return 0;
    }
  }

  std::string latest_put_key = "$";
  std::string new_value1 = std::to_string(timestamp) + "|" + value;
  std::string new_value2 = std::to_string(timestamp);

  batch.Put(key, new_value1);
  batch.Put(latest_put_key, new_value2);

  put_status = db->Write(rocksdb::WriteOptions(), &batch);
  old_value = get_value_timestamp;
  // put_status = db->Put(rocksdb::WriteOptions(), key, new_value1);

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

    if (key == "$" || key == "#")
      continue;

    parseValue(value, timestamp, old_value);

    if (server_timestamp < timestamp) {
      latest_keys.push_back({key, value});
    }
  }
  return latest_keys;
}

int keyValueStore::batched_write(
    std::vector<std::pair<std::string, std::string>> &key_value_batch) {
  rocksdb::WriteBatch batch;
  rocksdb::Status batch_status;

  for (int i = 0; i < key_value_batch.size(); i++) {
    batch.Put(key_value_batch[i].first, key_value_batch[i].second);
  }

  batch_status = db->Write(rocksdb::WriteOptions(), &batch);

  if (!batch_status.ok()) {
    std::cerr << "Error writing batch: " << batch_status.ToString()
              << std::endl;
    return -1;
  }

  return 0;
}

void ConsistentHashing::addServer(const std::string &server_name) {
  for (int i = 0; i < virtualServers_; ++i) {
    // std::string virtualServerName = server_name + "-" + std::to_string(i);
    std::size_t hash = hash_fn(server_name);
    ring_[hash] = server_name;
  }
}

void ConsistentHashing::removeServer(const std::string &server_name) {
  for (int i = 0; i < virtualServers_; ++i) {
    // std::string virtualServerName = server_name + "-" + std::to_string(i);
    std::size_t hash = hash_fn(server_name);
    ring_.erase(hash);
  }
}

std::string ConsistentHashing::getServer(const std::string &key) {

  if (ring_.empty()) {
    std::cerr << "No consisent hashing ring" << std::endl;
    exit(1);
  }

  std::size_t hash = hash_fn(key);
  auto it = ring_.upper_bound(hash);

  if (it == ring_.end()) {
    it = ring_.begin();
  }

  return it->second;
}

void ConsistentHashing::printServersRing() {
  for (auto &pair : ring_) {
    std::cout << "Hash, Server Address = " << pair.first << ", " << pair.second
              << std::endl;
  }
}