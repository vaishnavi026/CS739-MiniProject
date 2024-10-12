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
