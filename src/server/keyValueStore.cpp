#include "keyValueStore.h"
#include <iostream>
#include <mutex>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

keyValueStore::keyValueStore() {
  // Open the SQLite database and establish the connection
  int rc = sqlite3_open("kv.db", &db);

  if (rc) {
    fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
    exit(1);
  }

  // Create a kv table in the database
  tablename = "kv_store";
  const char *create_table_query =
      "CREATE TABLE IF NOT EXISTS kv_store (key TEXT PRIMARY KEY, value TEXT);";
  char *err_msg = nullptr;

  rc = sqlite3_exec(db, create_table_query, 0, 0, &err_msg);
  if (rc != SQLITE_OK) {
    fprintf(stderr, "SQL error: %s\n", err_msg);
    sqlite3_free(err_msg);
    exit(1);
  }

  const char *all_row_query = "SELECT key, value FROM kv_store;";
  sqlite3_stmt *Stmt;

  int sqlite_rc = sqlite3_prepare_v2(db, all_row_query, -1, &Stmt, nullptr);

  if (sqlite_rc != SQLITE_OK) {
    fprintf(stderr, "Loading table to map failed : %s\n", sqlite3_errmsg(db));
    exit(1);
  }

  while (sqlite3_step(Stmt) == SQLITE_ROW) {
    std::string key =
        reinterpret_cast<const char *>(sqlite3_column_text(Stmt, 0));
    std::string value =
        reinterpret_cast<const char *>(sqlite3_column_text(Stmt, 1));
    kv_map[key] = value;
  }

  sqlite3_finalize(Stmt);

  // reader_count = 0;
}

keyValueStore::~keyValueStore() {
  // Close the connection to the database
  sqlite3_close(db);
}

int keyValueStore::read(char *key, std::string &value) {
  // {
  //     std::unique_lock<std::mutex> lock(read_count_mutex);
  //     reader_count++;
  //     if(reader_count == 1)
  //         resource_mutex.lock();
  // }
  // // do the read
  // {
  //     std::unique_lock<std::mutex> lock(read_count_mutex);
  //     reader_count--;
  //     if(reader_count == 0)
  //         resource_mutex.unlock();

  // }
  // return 0;
  {
    std::unique_lock<std::mutex> lock(read_count_mutex);
    reader_count++;
    if (reader_count == 1)
      resource_mutex.lock();
  }

  // resource_mutex.lock();

  int rc;
  auto iter = kv_map.find(key);
  if (iter != kv_map.end()) {
    value = iter->second;
    rc = 0;
  } else {
    rc = 1;
  }
  {
    std::unique_lock<std::mutex> lock(read_count_mutex);
    reader_count--;
    if (reader_count == 0)
      resource_mutex.unlock();
  }
  // resource_mutex.unlock();
  return rc;

  const char *read_query = "SELECT value FROM kv_store WHERE KEY = ?;";
  sqlite3_stmt *Stmt;

  int sqlite_rc = sqlite3_prepare_v2(db, read_query, -1, &Stmt, nullptr);

  if (sqlite_rc != SQLITE_OK) {
    fprintf(stderr, "Read query : SQLite prepare error : %s\n",
            sqlite3_errmsg(db));
    rc = -1;
  }

  sqlite3_bind_text(Stmt, 1, key, -1, SQLITE_TRANSIENT);
  sqlite_rc = sqlite3_step(Stmt);

  if (sqlite_rc == SQLITE_ROW) {
    char *column = (char *)sqlite3_column_text(Stmt, 0);
    if (column) {
      value = column;
      rc = 0;
    } else {
      rc = 1;
    }
  } else if (sqlite_rc == SQLITE_DONE) {
    rc = 1;
  } else {
    fprintf(stderr, "Read Query : Failed to read from the database : %s\n",
            sqlite3_errmsg(db));
    rc = -1;
  }

  sqlite3_finalize(Stmt);

  resource_mutex.unlock();

  return rc;
}

int keyValueStore::write(char *key, char *value, std::string &old_value) {
  resource_mutex.lock();

  int rc;
  std::string read_value;
  auto iter = kv_map.find(key);
  if (iter != kv_map.end()) {
    rc = 0;
    read_value = iter->second;
  } else {
    rc = 1;
  }
  // const char *read_query = "SELECT value FROM kv_store WHERE KEY = ?;";
  const char *write_query;
  // const char *read_value;
  int sqlite_rc;

  // sqlite3_stmt *read_Stmt;
  sqlite3_stmt *write_Stmt;

  // sqlite_rc = sqlite3_prepare_v2(db, read_query, -1, &read_Stmt, nullptr);

  // if (sqlite_rc != SQLITE_OK) {
  //   fprintf(stderr, "Read in Write query prepare error : %s\n",
  //           sqlite3_errmsg(db));
  //   rc = -1;
  // }

  // sqlite3_bind_text(read_Stmt, 1, key, -1, SQLITE_TRANSIENT);
  // sqlite_rc = sqlite3_step(read_Stmt);

  // if (sqlite_rc == SQLITE_ROW) {
  //   read_value = (const char *)sqlite3_column_text(read_Stmt, 0);
  //   if (read_value)
  //     rc = 0;
  //   else
  //     rc = 1;
  // } else if (sqlite_rc == SQLITE_DONE) {
  //   rc = 1;
  // } else {
  //   fprintf(stderr,
  //           "Read in Write Query : Failed to read from the database : %s\n",
  //           sqlite3_errmsg(db));
  //   rc = -1;
  // }

  // Deciding whether to insert the key or update the key
  if (rc == 1) {
    write_query = "INSERT INTO kv_store (value, key) VALUES (?, ?);";
  } else {
    write_query = "UPDATE kv_store SET value = ? WHERE key = ?;";
  }

  if (rc == -1) {
    // Not writing if the read has failed
  } else if (rc == 0 && strcmp(value, read_value.c_str()) == 0) {
    old_value = read_value;
    // Skipping write because read value same as the value to be written
  } else {
    old_value = read_value;
    kv_map[key] = value;
    sqlite_rc = sqlite3_prepare_v2(db, write_query, -1, &write_Stmt, nullptr);
    if (sqlite_rc != SQLITE_OK) {
      fprintf(stderr, "Write in Write query prepare error : %s\n",
              sqlite3_errmsg(db));
    }

    sqlite3_bind_text(write_Stmt, 1, value, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(write_Stmt, 2, key, -1, SQLITE_TRANSIENT);

    sqlite_rc = sqlite3_step(write_Stmt);

    if (sqlite_rc != SQLITE_DONE) {
      fprintf(stderr,
              "Write in Write Query : Failed to read from the database : %s\n",
              sqlite3_errmsg(db));
    }
    sqlite3_finalize(write_Stmt);
  }

  // sqlite3_finalize(read_Stmt);

  resource_mutex.unlock();
  return rc;
}
