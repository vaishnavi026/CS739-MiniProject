#include "keyValueStore.h"
#include <iostream>
#include <mutex>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

keyValueStore::keyValueStore() {
  // Open the SQLite database and establish the connection
  // sqlite3_config(SQLITE_CONFIG_SERIALIZED);
  sqlite3* db0 = nullptr;
  sqlite3* db1 = nullptr;
  sqlite3* db2 = nullptr;
  sqlite3* db3 = nullptr;
  sqlite3* db4 = nullptr;
  sqlite3* db5 = nullptr;
  sqlite3* db6 = nullptr;
  sqlite3* db7 = nullptr;
  
  opendb("kv0.db",db0);
  opendb("kv1.db",db1);
  opendb("kv2.db",db2);
  opendb("kv3.db",db3);
  opendb("kv4.db",db4);
  opendb("kv5.db",db5);
  opendb("kv6.db",db6);
  opendb("kv7.db",db7);
  

  // int rc = sqlite3_open("kv1.db", &db1);

  // if (rc) {
  //   fprintf(stderr, "Can't open database kv1: %s\n", sqlite3_errmsg(db1));
  //   exit(1);
  // }

  // Create a kv table in the database
  tablename = "kv_store";
  const char *sql =
      "CREATE TABLE IF NOT EXISTS kv_store (key TEXT PRIMARY KEY, value TEXT);";
  // char *err_msg = nullptr;

  // rc = sqlite3_exec(db, sql, 0, 0, &err_msg);
  // if (rc != SQLITE_OK) {
  //   fprintf(stderr, "SQL error: %s\n", err_msg);
  //   sqlite3_free(err_msg);
  //   exit(1);
  // }

  execdb(db0,sql);
  execdb(db1,sql);
  execdb(db2,sql);
  execdb(db3,sql);
  execdb(db4,sql);
  execdb(db5,sql);
  execdb(db6,sql);
  execdb(db7,sql);
  

  // reader_count = 0;
}

keyValueStore::~keyValueStore() {
  // Close the connection to the database
  sqlite3_close(db0);
  sqlite3_close(db1);
  sqlite3_close(db2);
  sqlite3_close(db3);
  sqlite3_close(db4);
  sqlite3_close(db5);
  sqlite3_close(db6);
  sqlite3_close(db7);
  
}

void opendb(const std::string &db_name, sqlite3* &db){
    int rc = sqlite3_open(db_name.c_str(), &db);

    if (rc) {
        fprintf(stderr, "Can't open database %s: %s\n", db_name.c_str(), sqlite3_errmsg(db));
        exit(1);
    }
}

void execdb(sqlite3* db, const char* sql){
    
    char *err_msg = nullptr;
    int rc = sqlite3_exec(db, sql, 0, 0, &err_msg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);
        exit(1);
    }
}

int readdb(sqlite3* db,char *key, std::string &value){
    
    int rc;
    const char *read_query = "SELECT value FROM kv_store WHERE KEY = ?;";
    sqlite3_stmt *Stmt = nullptr;

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

    return rc;
}

int writedb(sqlite3* db,char *key, char *value, std::string &old_value){
    
    int rc;
    const char *read_query = "SELECT value FROM kv_store WHERE KEY = ?;";
    const char *write_query;
    const char *read_value;

    sqlite3_stmt *read_Stmt = nullptr;
    sqlite3_stmt *write_Stmt = nullptr;

    int sqlite_rc = sqlite3_prepare_v2(db, read_query, -1, &read_Stmt, nullptr);

    if (sqlite_rc != SQLITE_OK) {
        fprintf(stderr, "Read in Write query prepare error : %s\n",
        sqlite3_errmsg(db));
        rc = -1;
    }

    sqlite3_bind_text(read_Stmt, 1, key, -1, SQLITE_TRANSIENT);
    sqlite_rc = sqlite3_step(read_Stmt);

    if (sqlite_rc == SQLITE_ROW) {
        read_value = (const char *)sqlite3_column_text(read_Stmt, 0);
        if (read_value)
            rc = 0;
        else
            rc = 1;
    } else if (sqlite_rc == SQLITE_DONE) {
        rc = 1;
    } else {
        fprintf(stderr,
                "Read in Write Query : Failed to read from the database : %s\n",
                sqlite3_errmsg(db));
        rc = -1;
    }

    // Deciding whether to insert the key or update the key
    if (rc == 1) {
        write_query = "INSERT INTO kv_store (value, key) VALUES (?, ?);";
    } else {
        write_query = "UPDATE kv_store SET value = ? WHERE key = ?;";
    }

    if (rc == -1) {
        // Not writing if the read has failed
    } else if (rc == 0 && strcmp(value, read_value) == 0) {
        old_value = read_value;
        // Skipping write because read value same as the value to be written
    } else {
        old_value = read_value;
        sqlite_rc = sqlite3_prepare_v2(db, write_query, -1, &write_Stmt, nullptr);
        if (sqlite_rc != SQLITE_OK) {
            fprintf(stderr, "Write in Write query prepare error : %s\n",sqlite3_errmsg(db));
        }

        sqlite3_bind_text(write_Stmt, 1, value, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(write_Stmt, 2, key, -1, SQLITE_TRANSIENT);

        sqlite_rc = sqlite3_step(write_Stmt);

        if (sqlite_rc != SQLITE_DONE) {
            fprintf(stderr,"Write in Write Query : Failed to read from the database : %s\n",sqlite3_errmsg(db));
        }
        sqlite3_finalize(write_Stmt);
    }

    sqlite3_finalize(read_Stmt);

}

int keyValueStore::read(char *key, std::string &value) {

    size_t hash_value = hash_fn(key);
    int db_idx = (int)hash_value % 8;
    int rc;
    
    switch(db_idx){
        case 0:
            db_mutexes[0].lock();
            rc = readdb(db0,key,value);
            db_mutexes[0].unlock();
            break;
        case 1:
            db_mutexes[1].lock();
            rc = readdb(db1,key,value);
            db_mutexes[1].unlock();
            break;
        case 2:
            db_mutexes[2].lock();
            rc = readdb(db2,key,value);
            db_mutexes[2].unlock();
            break;
        case 3: 
            db_mutexes[3].lock();
            rc = readdb(db3,key,value);
            db_mutexes[3].unlock();
            break;
        case 4:
            db_mutexes[4].lock();
            rc = readdb(db4,key,value);
            db_mutexes[4].unlock();
            break;
        case 5:
            db_mutexes[5].lock();
            rc = readdb(db5,key,value);
            db_mutexes[5].unlock();
            break;
        case 6:
            db_mutexes[6].lock();
            rc = readdb(db6,key,value);
            db_mutexes[6].unlock();
            break;
        case 7:
            db_mutexes[7].lock();
            rc = readdb(db7,key,value);
            db_mutexes[7].unlock();
            break;
    }

    return rc;

}

int keyValueStore::write(char *key, char *value, std::string &old_value) {
    
    size_t hash_value = hash_fn(key);
    int db_idx = (int)hash_value % 8;
    int rc;
    
    switch(db_idx){
        case 0:
            db_mutexes[0].lock();
            rc = writedb(db0,key,value,old_value);
            db_mutexes[0].unlock();
            break;
        case 1:
            db_mutexes[1].lock();
            rc = writedb(db1,key,value,old_value);
            db_mutexes[1].unlock();
            break;
        case 2:
            db_mutexes[2].lock();
            rc = writedb(db2,key,value,old_value);
            db_mutexes[2].unlock();
            break;
        case 3: 
            db_mutexes[3].lock();
            rc = writedb(db3,key,value,old_value);
            db_mutexes[3].unlock();
            break;
        case 4:
            db_mutexes[4].lock();
            rc = writedb(db4,key,value,old_value);
            db_mutexes[4].unlock();
            break;
        case 5:
            db_mutexes[5].lock();
            rc = writedb(db5,key,value,old_value);
            db_mutexes[5].unlock();
            break;
        case 6:
            db_mutexes[6].lock();
            rc = writedb(db6,key,value,old_value);
            db_mutexes[6].unlock();
            break;
        case 7:
            db_mutexes[7].lock();
            rc = writedb(db7,key,value,old_value);
            db_mutexes[7].unlock();
            break;
    }
}
