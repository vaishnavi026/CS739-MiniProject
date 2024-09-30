#ifndef DBCONNECTIONPOOL_H
#define DBCONNECTIONPOOL_H

#include <condition_variable>
#include <mutex>
#include <queue>
#include <sqlite3.h>

class DBConnectionPool {
public:
  DBConnectionPool(size_t poolSize) {
    sqlite3_config(SQLITE_CONFIG_MULTITHREAD);
    for (size_t i = 0; i < poolSize; ++i) {
      sqlite3 *db;
      if (sqlite3_open("kv.db", &db) == SQLITE_OK) {
        connections.push(db);
      } else {
        fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
      }
    }
  }

  ~DBConnectionPool() {
    while (!connections.empty()) {
      sqlite3 *db = connections.front();
      connections.pop();
      sqlite3_close(db);
    }
  }

  sqlite3 *getConnection() {
    std::unique_lock<std::mutex> lock(mutex);
    while (connections.empty()) {
      condition.wait(lock);
    }
    sqlite3 *db = connections.front();
    connections.pop();
    return db;
  }

  void releaseConnection(sqlite3 *db) {
    std::unique_lock<std::mutex> lock(mutex);
    connections.push(db);
    condition.notify_one();
  }

private:
  std::queue<sqlite3 *> connections;
  std::mutex mutex;
  std::condition_variable condition;
};

#endif