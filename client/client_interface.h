#ifndef KV739CLIENT_H
#define KV739CLIENT_H

int kv739_init(char *server_name);

int kv739_shutdown(void);

int kv739_get(char *key, char *value);

int kv739_put(char *key, char *value, char *old_value);

int kv739_die(char *server_name, int clean);

#endif