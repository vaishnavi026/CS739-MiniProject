#include "keyValueStore.h"
#include <iostream>
#include <mutex>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

keyValueStore::keyValueStore() {
  options.create_if_missing = true;
  std::string db_name = "rocksdb";
  rocksdb::Status status = rocksdb::DB::Open(options, db_name, &db);
  if (!status.ok()) {
    std::cerr << "Error opening database: " << status.ToString() << std::endl;
    exit(1);
  }
}

keyValueStore::keyValueStore(std::string server_address) {
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
                         std::string &old_value) {
  rocksdb::Status get_status;
  rocksdb::Status put_status;
  get_status = db->Get(rocksdb::ReadOptions(), key, &old_value);

  put_status = db->Put(rocksdb::WriteOptions(), key, value);

  if (!put_status.ok()) {
    std::cerr << "Error putting value: " << put_status.ToString() << std::endl;
    return -1;
  }

  if (!get_status.ok()) {
    return 1;
  }

  return 0;
}
