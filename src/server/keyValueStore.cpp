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

int keyValueStore::get(char *key, char *value){
    {
        std::unique_lock<std::mutex> lock(m);
        while(is_writing == true)
            c.wait(lock);
        reader_count++;
    }
    // do the read
    {
        std::unique_lock<std::mutex> lock(m);
        reader_count--;
        if(reader_count == 0)
            c.notify_all();
        
    }
    return 0;

}

