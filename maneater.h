#ifndef MANEATER_H
#define MANEATER_H

#include <stdlib.h>
#include <czmq.h>
#include <pthread.h>
#include <msgpack.h>

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

enum {
    MID_SET = 100,
    MID_VALUE,
    MID_WANT_MASTER,
    MID_IS_MASTER,
    MID_C_ALIVE,
    MID_S_ALIVE
};



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
    pthread_t thread;
} maneater_client;

maneater_client * maneater_client_new(
char *iface, char **hosts, int num_hosts);

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

#define MHASH_STRING_SET(h, f, o) ({\
    HASH_ADD_KEYPTR(hh, h, o->f, strlen(o->f), o);\
    })

#endif /* MANEATER_H */
