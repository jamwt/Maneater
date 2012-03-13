#ifndef MANEATER_H
#define MANEATER_H

#include <stdlib.h>
#include <czmq.h>
#include <pthread.h>
#include <msgpack.h>

#include "uthash.h"

/* SERVER */
void handle_args(int argc, char **argv);
void setup_node_state();
void setup_zeromq();
void loop();

typedef enum {
    ROLE_ELECTING,
    ROLE_SLAVE,
    ROLE_MASTER
} ROLE;

#define NODEID_LEN (8 + UUID_SIZE + 1)
#define HOSTID_LEN (150)

// MAXINT(64)
#define START_TXID 0xffffffffffffffff

typedef struct {
    char *host;
    void *peer_socket;
} consensus_host;

/* SETS */

void start_master(zctx_t *ctx, consensus_host *slaves, int num_slaves);
void start_slave(zctx_t *ctx, void *master_socket);

void end_master(zctx_t *ctx);
void end_slave(zctx_t *ctx);

int send_to_host(const char *host, zframe_t *frame);
void set_init();
void handle_set_message(unsigned char * data, size_t len);
void handle_follow_message(unsigned char * data, size_t len);

enum {
    MID_SET = 100,
    MID_FOLLOW,
    MID_VALUE,
    MID_WANT_MASTER,
    MID_IS_MASTER,
    MID_C_ALIVE,
    MID_S_ALIVE
};

typedef struct {
    char *key;
    void *value;
    int value_length;
    int lock;
    uint64_t limit;
} proc_message_set;

typedef struct {
    char *key;
    int own;
    proc_message_set *set;

    UT_hash_handle hh;
} client_want_set;

typedef struct {
    char *key;
    uint64_t txid;

    UT_hash_handle hh;
} client_follow;

/* CLIENT */
typedef struct {
    void * local_socket;
    zctx_t *ctx;
    char **hosts;
    int num_hosts;
    char *master;
    void *master_socket;
    char *iface;
    char *myhostid;
    void **sockets;
    char *sessid;
    int last_tick;
    void *proc_socket;
    pthread_t thread;
    client_follow *follows;
    client_want_set *wants;
    void *cb; // actually =  maneater_value_callback
} maneater_client;

// ptr = void *
// size = length
typedef struct {
    uint32_t size;
    void *ptr;
} maneater_bin;

typedef void(*maneater_value_callback) (
    maneater_client *cli,
    const char *key,
    maneater_bin *values,
    int num_values);

maneater_client * maneater_client_new(
char *iface, char **hosts, int num_hosts,
maneater_value_callback cb
);

void maneater_client_sub(
maneater_client * cli,
const char *key);

void maneater_client_set(maneater_client * cli,
        const char *key,
        const void *value,
        int value_length,
        int lock,
        uint64_t limit
        );

/* XXX unsub */


/* HELPER MACROS */

#define MSG_NEXT(msg, data, size, off) ({\
    *off = 0;\
    msgpack_unpacked_init(msg);\
    msgpack_unpack_next(msg, (char *)data, size, off); \
    assert(off > 0);\
    size -= *off; \
    data += *off; \
    })

/* include the trailing NULL byte for safe string transport */
#define MSG_PACK_STR(pk, s) ({\
    msgpack_pack_raw(pk, strlen(s) + 1); \
    msgpack_pack_raw_body(pk, s, strlen(s) + 1);\
    })

#define MSG_START(pk, buf) ({\
    msgpack_sbuffer * buf = msgpack_sbuffer_new();\
    msgpack_packer * pk = msgpack_packer_new(buf, msgpack_sbuffer_write);\
    })

#define MSG_END(pk, buf) ({\
    msgpack_packer_free(pk);\
    msgpack_sbuffer_free(buf);\
    })

#define MSG_DOPACK(block) ({\
    msgpack_sbuffer * buf = msgpack_sbuffer_new();\
    msgpack_packer * pk = msgpack_packer_new(buf, msgpack_sbuffer_write);\
    block\
    msgpack_packer_free(pk);\
    msgpack_sbuffer_free(buf);\
    })

#define COPY_STRING(d, s) ({\
    d = malloc(strlen(s) + 1);\
    strcpy(d, s);\
    })

#define COPY_MEM(d, s, l) ({\
    d = malloc(l);\
    memcpy(d, (const void*)s, l);\
    })

#define MHASH_STRING_SET(h, f, o) ({\
    HASH_ADD_KEYPTR(hh, h, o->f, strlen(o->f), o);\
    })

#endif /* MANEATER_H */
