#include <stdlib.h>
#include <czmq.h>
#include <pthread.h>
#include <assert.h>
#include <sys/time.h>

#include "uuid.h"
#include "maneater.h"

#define MASTER_TIMEOUT 6
#define INPROC_PATH "inproc://request"

typedef enum {
    CS_FINDMASTER,
    CS_SYNC
} CLIENT_STATE;

enum {
    PROC_MID_SUB,
    PROC_MID_SET,
    PROC_MID_GET,
    PROC_MID_DEL,
};

typedef struct {
    char *key;
} proc_message_sub;

typedef struct {
    char *key;
} proc_message_get;

typedef struct {
    char *key;
    void *value;
    int value_length;
} proc_message_del;

typedef struct {
    int mid;

    union data {
        proc_message_sub sub;
        proc_message_set set;
        proc_message_get get;
        proc_message_del del;
    } via;
} proc_message;

static int random_init;

int find_master_input(zloop_t *loop, zmq_pollitem_t *item, void *arg) {
    maneater_client *cli = (maneater_client *)arg;
    zframe_t *incoming = zframe_recv(cli->local_socket);
    unsigned char *data = zframe_data(incoming);
    int size = zframe_size(incoming);
    msgpack_unpacked msg;
    size_t off;
    MSG_NEXT(&msg, data, size, &off);
    assert(msg.data.type == MSGPACK_OBJECT_POSITIVE_INTEGER);
    uint64_t msgid = msg.data.via.u64;

    int ret = 0, i = 0;

    if (msgid == MID_IS_MASTER) {
        MSG_NEXT(&msg, data, size, &off);
        assert(msg.data.type == MSGPACK_OBJECT_RAW);
        char *host;
        COPY_STRING(host, msg.data.via.raw.ptr);

        cli->master = host;

        cli->master_socket = NULL;
        for (i=0; i < cli->num_hosts; i++) {
            if (!strcmp(cli->hosts[i], host)) {
                cli->master_socket = cli->sockets[i];
                break;
            }
        }

        assert(cli->master_socket);
        struct timeval tim;
        gettimeofday(&tim, NULL);
        cli->last_tick = tim.tv_sec;

        zclock_log("got master: %s", host);

        ret = -1;
    }

    zframe_destroy(&incoming);
    return ret;
}

int master_finder(zloop_t *loop, zmq_pollitem_t *item, void *arg) {
    maneater_client *cli = (maneater_client *)arg;
    zframe_t *out;

    MSG_DOPACK(
        msgpack_pack_int(pk, MID_WANT_MASTER);
        MSG_PACK_STR(pk, cli->myhostid);
        out = zframe_new(buf->data, buf->size);
    );

    int i;

    for (i=0; i < cli->num_hosts; i++) {
        zframe_t *copy = zframe_dup(out);
        zframe_send(&copy, cli->sockets[i], 0);
    }
    zframe_destroy(&out);

    return 0;
}

CLIENT_STATE find_master(maneater_client * cli) {
    zloop_t * mainloop = zloop_new();
    zmq_pollitem_t bind_input = {cli->local_socket, 0, ZMQ_POLLIN};
    zloop_poller(mainloop, &bind_input, find_master_input, (void *)cli);
    zloop_timer(mainloop, 1000, 0, master_finder, (void *)cli);

    zloop_start(mainloop);

    // after master is found...
    zloop_destroy(&mainloop);

    return CS_SYNC;
}

void handle_server_alive(unsigned char *data, int size, maneater_client *cli) {
    struct timeval tim;
    gettimeofday(&tim, NULL);
    cli->last_tick = tim.tv_sec;
}

void send_set_request(maneater_client *cli, proc_message_set *s) {

    zframe_t *out = NULL;

    MSG_DOPACK(
        msgpack_pack_int(pk, MID_SET);
        MSG_PACK_STR(pk, cli->myhostid);
        MSG_PACK_STR(pk, s->key);
        if (s->lock)
            MSG_PACK_STR(pk, cli->sessid);
        else
            msgpack_pack_nil(pk);
        msgpack_pack_uint64(pk, s->limit);
        msgpack_pack_raw(pk, s->value_length);
        msgpack_pack_raw_body(pk, s->value, s->value_length);

        out = zframe_new(buf->data, buf->size);
    );
        
    zframe_send(&out, cli->master_socket, 0);
}

void handle_value(unsigned char *data, int size, maneater_client *cli) {
    
    size_t off;
    msgpack_unpacked msg;

    MSG_NEXT(&msg, data, size, &off);
    assert(msg.data.type == MSGPACK_OBJECT_RAW);
    const char *key = msg.data.via.raw.ptr;

    MSG_NEXT(&msg, data, size, &off);
    assert(msg.data.type == MSGPACK_OBJECT_POSITIVE_INTEGER);
    uint64_t txid = msg.data.via.u64;
    printf("%s %d\n", key, (int)txid);

    MSG_NEXT(&msg, data, size, &off);
    printf("%d\n", msg.data.type);
    assert(msg.data.type == MSGPACK_OBJECT_ARRAY);

    int i = 0, count = msg.data.via.array.size;

    maneater_bin * results = (maneater_bin *) malloc(count * sizeof(maneater_bin));
    printf("getting %d values for %s\n", count, key);

    for (i=0; i < count; i++) {
        msgpack_object * obj = &msg.data.via.array.ptr[i];
        assert(obj->type == MSGPACK_OBJECT_RAW);

        results[i].ptr = (char *)obj->via.raw.ptr;
        results[i].size = obj->via.raw.size;
    }

    // XXX update follows 
    client_follow *follow = NULL;
    HASH_FIND_STR(cli->follows, key, follow);
    if (follow) {
        // finally, here's your data!
        follow->txid = txid; // updated!
        ((maneater_value_callback)cli->cb)(cli, key, results, count);
    }
    client_want_set *want = NULL;
    HASH_FIND_STR(cli->wants, key, want);
    if (want && !want->own) {
        printf("got stuff!\n");
        for (i=0; i < count; i++) {
            if (results[i].size == want->set->value_length && 
            !memcmp(want->set->value, results[i].ptr, results[i].size)) {
                want->own = 1;
                break;
            }
        }

        if (!want->own && 
        (want->set->limit == 0 || count < want->set->limit))
            send_set_request(cli, want->set);
    }

    free(results);
}


int steady_network_input(zloop_t *loop, zmq_pollitem_t *item, void *arg) {
    /* handle packets from network */
    maneater_client *cli = (maneater_client *)arg;
    zframe_t *incoming = zframe_recv(cli->local_socket);
    unsigned char *data = zframe_data(incoming);
    int size = zframe_size(incoming);
    msgpack_unpacked msg;
    size_t off;
    MSG_NEXT(&msg, data, size, &off);
    assert(msg.data.type == MSGPACK_OBJECT_POSITIVE_INTEGER);
    uint64_t msgid = msg.data.via.u64;

    switch (msgid) {
        case MID_S_ALIVE: handle_server_alive(data, size, cli); break;
        case MID_IS_MASTER: break;
        case MID_VALUE: handle_value(data, size, cli); break;
        default: fprintf(stderr, "eh? %d\n", (int)msgid); assert(0); /* unknown message type */
    }

    return 0;
}

int master_pinger(zloop_t *loop, zmq_pollitem_t *item, void *arg) {
    /* handle packets from network */
    maneater_client *cli = (maneater_client *)arg;
    zframe_t *out;

    struct timeval tim;
    gettimeofday(&tim, NULL);
    if (tim.tv_sec - cli->last_tick > MASTER_TIMEOUT) {
        zclock_log("MASTER TIMEOUT!\n");
        return -1;
    }

    MSG_DOPACK(
        msgpack_pack_int(pk, MID_C_ALIVE);
        MSG_PACK_STR(pk, cli->myhostid);
        MSG_PACK_STR(pk, cli->sessid);

        out = zframe_new(buf->data, buf->size);
    );

    zframe_send(&out, cli->master_socket, 0);

    return 0;
}

void send_updated_follow_list(maneater_client *cli) {
    zframe_t *out = NULL;

    int num_follows = HASH_COUNT(cli->follows);

    client_follow *follow = NULL;

    MSG_DOPACK(
        msgpack_pack_int(pk, MID_FOLLOW);
        MSG_PACK_STR(pk, cli->myhostid);
        msgpack_pack_array(pk, num_follows * 2);

        for (follow = cli->follows; follow; follow = follow->hh.next) {
            MSG_PACK_STR(pk, follow->key);
            msgpack_pack_uint64(pk, follow->txid);
        }

        out = zframe_new(buf->data, buf->size);
    );

    zframe_send(&out, cli->master_socket, 0);
}

void handle_sub(maneater_client *cli, proc_message_sub *sub) {
    printf("wants sub on %s\n", sub->key);

    client_follow * follow = NULL;

    HASH_FIND_STR(cli->follows, sub->key, follow);
    int used_key = 0;

    if (!follow) {
        follow = (client_follow *) malloc(sizeof(client_follow));
        follow->key = sub->key;
        follow->txid = START_TXID;
        used_key = 1;
        MHASH_STRING_SET(cli->follows, key, follow);
        send_updated_follow_list(cli);
    }

    if (!used_key)
        free(sub->key);
}

void handle_set(maneater_client *cli, proc_message_set *set) {
    printf("wants set on %s\n", set->key);

    client_want_set *want = NULL;

    HASH_FIND_STR(cli->wants, set->key, want);
    int used = 0;
    if (!want) {
        printf("adding!\n");
        used = 1;
        want = (client_want_set*)malloc(sizeof(client_want_set));
        want->key = set->key;
        want->own = 0;
        COPY_MEM(want->set, set, sizeof(proc_message_set));
        MHASH_STRING_SET(cli->wants, key, want);
        proc_message_sub sub;
        COPY_STRING(sub.key, want->key);
        handle_sub(cli, &sub);
        send_set_request(cli, want->set);
    }

    if (!used) {
        free(set->key);
        free(set->value);
    }
}

void handle_get(maneater_client *cli, proc_message_get *get) {
    zframe_t *out;

    MSG_DOPACK(
        msgpack_pack_int(pk, MID_GET);
        MSG_PACK_STR(pk, cli->myhostid);
        MSG_PACK_STR(pk, get->key);

        out = zframe_new(buf->data, buf->size);
    );

    zframe_send(&out, cli->master_socket, 0);

    free(get->key);
}

void handle_del(maneater_client *cli, proc_message_del *del) {
    zframe_t *out;

    MSG_DOPACK(
        msgpack_pack_int(pk, MID_DEL);
        MSG_PACK_STR(pk, del->key);
        if (del->value) {
            msgpack_pack_raw(pk, del->value_length);
            msgpack_pack_raw_body(pk, del->value, del->value_length);
        }
        else
            msgpack_pack_nil(pk);

        out = zframe_new(buf->data, buf->size);
    );

    zframe_send(&out, cli->master_socket, 0);

    free(del->key);

    if (del->value)
        free(del->value);
}

int steady_inproc_input(zloop_t *loop, zmq_pollitem_t *item, void *arg) {
    maneater_client *cli = (maneater_client *)arg;
    zframe_t *incoming = zframe_recv(cli->proc_socket);
    proc_message *msg = (proc_message *)zframe_data(incoming);
    switch (msg->mid) {
        case PROC_MID_SUB: handle_sub(cli, &(msg->via.sub)); break;
        case PROC_MID_SET: handle_set(cli, &(msg->via.set)); break;
        case PROC_MID_GET: handle_get(cli, &(msg->via.get)); break;
        case PROC_MID_DEL: handle_del(cli, &(msg->via.del)); break;
        default: assert(0);
    }

    zframe_destroy(&incoming);

    return 0; 
}

int follow_loop(zloop_t *loop, zmq_pollitem_t *item, void *arg) {
    maneater_client *cli = (maneater_client *)arg;
    send_updated_follow_list(cli);
    return 0;
}

CLIENT_STATE steady_state(maneater_client * cli) {
    zloop_t * mainloop = zloop_new();
    /* also inproc:// */
    zmq_pollitem_t bind_input = {cli->local_socket, 0, ZMQ_POLLIN};
    zmq_pollitem_t inproc_input = {cli->proc_socket, 0, ZMQ_POLLIN};
    zloop_poller(mainloop, &bind_input, steady_network_input, (void *)cli);
    zloop_poller(mainloop, &inproc_input, steady_inproc_input, (void *)cli);
    zloop_timer(mainloop, 1000, 0, master_pinger, (void *)cli);
    zloop_timer(mainloop, 10000, 0, follow_loop, (void *)cli);

    zloop_start(mainloop);

    // after master is lost
    zloop_destroy(&mainloop);

    free(cli->master);
    cli->master = NULL;
    cli->master_socket = NULL; // no master socket
    cli->last_tick = 0;

    return CS_FINDMASTER;
}

void * client_run_loop(void *data) {
    maneater_client * cli = (maneater_client *)data;
    int i;
    void *local_socket = zsocket_new(cli->ctx, ZMQ_DEALER);
    int port = zsocket_bind(local_socket, "tcp://%s:*", cli->iface);

    cli->myhostid = (char *)malloc(strlen(cli->iface) + 10);

    snprintf(cli->myhostid, strlen(cli->iface) + 10, "%s:%d", cli->iface, port);
    cli->sockets = (void **)malloc(sizeof(void *) * cli->num_hosts);

    for (i=0; i < cli->num_hosts; i++) {
        void * s = zsocket_new(cli->ctx, ZMQ_DEALER);
        zsocket_connect(s, "tcp://%s", cli->hosts[i]); // XXX error checking?
        cli->sockets[i] = s;
    }

    cli->local_socket = local_socket;

    CLIENT_STATE cs = CS_FINDMASTER;

    while (1) {
        switch (cs) {
            case CS_FINDMASTER: cs = find_master(cli); break;
            case CS_SYNC: cs = steady_state(cli); break;
            default: assert(0);
        }
    }

    return NULL;
}

maneater_client * maneater_client_new(
char *iface, char **hosts, int num_hosts,
maneater_value_callback cb
) {
    if (!random_init) {
        init_random_system();
        random_init = 1;
    }
    int i;
    maneater_client * cli;

    cli = (maneater_client *)malloc(sizeof(maneater_client));
    cli->follows = NULL;
    cli->wants = NULL;
    cli->sessid = (char *)malloc(UUID_SIZE + 1);
    cli->cb = (void *)cb;
    generate_uuid4(cli->sessid);
    cli->iface = iface;
    cli->ctx = zctx_new();
    cli->num_hosts = num_hosts;
    cli->hosts = (char **)malloc(sizeof(char *) * num_hosts);
    for (i=0; i < num_hosts; i++) {
        COPY_STRING(cli->hosts[i], hosts[i]);
    }

    cli->proc_socket = zsocket_new(cli->ctx, ZMQ_DEALER);
    zsocket_bind(cli->proc_socket, INPROC_PATH);

    pthread_create(&(cli->thread), NULL, client_run_loop, (void *)cli);

    return cli;
}

#define CLIENT_INPROC_SEND(cli, msg) ({\
    void * tsock = zsocket_new(cli->ctx, ZMQ_DEALER);\
    zsocket_connect(tsock, INPROC_PATH);\
    zframe_t * frame = zframe_new((void *)&msg, sizeof(msg));\
    zframe_send(&frame, tsock, 0);\
    zframe_destroy(&frame);\
    printf("sent!\n");\
    })

void maneater_client_sub(maneater_client *cli, 
        const char *key) {
    proc_message msg;
    msg.mid = PROC_MID_SUB;
    COPY_STRING(msg.via.sub.key, key);

    CLIENT_INPROC_SEND(cli, msg);

}
void maneater_client_set(maneater_client * cli, 
        const char *key,
        const void *value,
        int value_length,
        int lock,
        uint64_t limit
        ) {
    proc_message msg;
    proc_message_set *s;
    msg.mid = PROC_MID_SET;
    s = &msg.via.set;
    COPY_STRING(s->key, key);
    COPY_MEM(s->value, value, value_length);
    s->value_length = value_length;
    s->lock = lock;
    s->limit = limit;

    CLIENT_INPROC_SEND(cli, msg);
}

void maneater_client_get(maneater_client * cli, 
        const char *key
        ) {
    proc_message msg;
    msg.mid = PROC_MID_GET;
    COPY_STRING(msg.via.get.key, key);

    CLIENT_INPROC_SEND(cli, msg);
}

void maneater_client_del(maneater_client * cli, 
        const char *key,
        char *value,
        int value_length
        ) {
    proc_message msg;
    msg.mid = PROC_MID_DEL;
    COPY_STRING(msg.via.del.key, key);
    if (value) {
        COPY_STRING(msg.via.del.value, value);
        msg.via.del.value_length = value_length;
    }
    else
        msg.via.del.value = NULL;

    CLIENT_INPROC_SEND(cli, msg);
}
