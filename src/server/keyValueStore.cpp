#include "keyValueStore.h"
#include <iostream>
#include <mutex>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

keyValueStore::keyValueStore() {
  options.create_if_missing = true;
  rocksdb::Status status = rocksdb::DB::Open(options, "rocksdb", &db);
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
    std::cout << "Value for key: " << value << std::endl;
    return 0;
  } else if (status.IsNotFound()) {
    std::cout << "Key not found" << std::endl;
    return 1;
  }

  std::cerr << "Error getting value: " << status.ToString() << std::endl;
  return -1;
}

int keyValueStore::write(const std::string &key, const std::string &value,
                         std::string &old_value) {
  rocksdb::Status status;
  status = db->Put(rocksdb::WriteOptions(), key, value);

  if (!status.ok()) {
    std::cerr << "Error putting value: " << status.ToString() << std::endl;
    return 0;
  }

  return 1;
}
