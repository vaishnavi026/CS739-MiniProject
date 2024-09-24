#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "keyValueStore.h"
#include <sqlite3.h>
#include <mutex>

keyValueStore::keyValueStore()
{
    // Open the SQLite database and establish the connection

    int rc = sqlite3_open("kv.db", &db);
   
    if(rc) {
        fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
        exit(1);
    } 

    // Create a kv table in the database
    tablename = "kv_store";
    const char *sql = "CREATE TABLE IF NOT EXISTS kv_store (key TEXT PRIMARY KEY, value TEXT);";
    char *err_msg = nullptr;

    rc = sqlite3_exec(db, sql, 0, 0, &err_msg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);
        exit(1);
    }
    reader_count = 0;
}


keyValueStore::~keyValueStore()
{
    // Close the connection to the database
    sqlite3_close(db);
}

int keyValueStore::read(char *key, char *value){
    {
        std::unique_lock<std::mutex> lock(read_count_mutex);
        reader_count++;
        if(reader_count == 1)
            resource_mutex.lock();
    }
    // do the read
    {
        std::unique_lock<std::mutex> lock(read_count_mutex);
        reader_count--;
        if(reader_count == 0)
            resource_mutex.unlock();
        
    }
    return 0;

}

char* keyValueStore::write(char *key, char *value){
    resource_mutex.lock();
    //do the writing
    resource_mutex.unlock();

}

