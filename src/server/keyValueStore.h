#ifndef KEYVALUESTORE_H
#define KEYVALUESTORE_H

#include <mutex>
#include <sqlite3.h>
class keyValueStore {
private:
  sqlite3 *db0;
  sqlite3 *db1;   
  sqlite3 *db2; 
  sqlite3 *db3; 
  sqlite3 *db4; 
  sqlite3 *db5; 
  sqlite3 *db6; 
  sqlite3 *db7;  
  char *tablename; // Name of the table in DB (Storing key values persistently)
  //std::mutex resource_mutex;
  std::mutex db_mutexes[8];
  std::hash<char*> hash_fn;
  //std::mutex read_count_mutex;
  //int reader_count = 0;

  int readdb(sqlite3 *db, char *key, std::string &value);
  int writedb(sqlite3 *db, char *key, char *value, std::string &old_value);
  void opendb(const std::string &db_name, sqlite3* &db);
  void execdb(sqlite3* db, const char* sql);
public:
  // Constructor and Destructor
  keyValueStore();
  ~keyValueStore();

  // Member functions to get and put key-value pairs
  int read(char *key, std::string &value);
  int write(char *key, char *value, std::string &old_value);
};

#endif // KEYVALUESTORE_H
