#include <czmq.h>

#include "maneater.h"
#include "uthash.h"
#include "utlist.h"

typedef struct stored_value {
    char *lock;
    msgpack_object_raw data;

    /* for utlist.h */
    struct stored_value *prev;
    struct stored_value *next;
} stored_value;

typedef struct {
    char * key;
    stored_value * values;
    unsigned long long txid;
    int num_values;
    UT_hash_handle hh;
} stored_set;

typedef struct {
    char *host;
    void *peer_socket;
    UT_hash_handle hh;
} peer_map;

typedef struct sublist {
    void *client_socket;
    char *host;

    UT_hash_handle hh;
} sublist;

typedef struct {
    char *key;
    sublist *subs;
    UT_hash_handle hh;
} key_sub;


typedef struct {
    ROLE role;
    zctx_t *ctx;
    peer_map *slaves;
    int num_slaves;
    stored_set *db;
    key_sub *subs;
    sublist *hostsockets;
} set_state;

static unsigned long long txid;
static set_state sstate;


void set_init() {
    txid = 0;
    sstate.role = ROLE_ELECTING;
    sstate.slaves = NULL;
    sstate.num_slaves = 0;
    sstate.db = NULL;
}

sublist * check_subber_indexed(const char *host, int * added) {
    sublist *subber;
    HASH_FIND_STR(sstate.hostsockets, host, subber);
    
    *added = 0;
    if (!subber) {
        subber = (sublist *)malloc(sizeof (sublist));
        subber->host = malloc(strlen(host));
        strcpy(subber->host, host);
        *added = 1;

        subber->client_socket = zsocket_new(sstate.ctx, ZMQ_DEALER);
        zsocket_connect(subber->client_socket, "tcp://%s", host);
        HASH_ADD_STR(sstate.hostsockets, host, subber);
    }

    return subber;
}


int send_to_host(const char *host, zframe_t *frame) {
    int i;
    sublist * subber = check_subber_indexed(host, &i);

    assert(subber);

    zframe_send(&frame, subber->client_socket, 0);

    return i;
}

void fire_update(stored_set *s, void *msock) {
    zframe_t *template, *send;
    int i;

    MSG_DOPACK(
        msgpack_pack_int(pk, MID_VALUE);
        MSG_PACK_STR(pk, s->key);
        msgpack_pack_uint64(pk, s->txid);
        msgpack_pack_array(pk, s->num_values);

        for (i=0; i < s->num_values; i++) {
            msgpack_pack_raw(pk, s->values[i].data.size);
            msgpack_pack_raw_body(pk,
                s->values[i].data.ptr,
                s->values[i].data.size);
        }

        template = zframe_new(buf->data, buf->size);
    );

    if (msock) {
        send = zframe_dup(template);
        zframe_send(msock, send, 0);
    }
    else {
        if (sstate.role == ROLE_MASTER) {
            for (i=0; i < sstate.num_slaves; i++) {
                send = zframe_dup(template);
                zframe_send(sstate.slaves[i].peer_socket, send, 0);
            }
        }

        key_sub *keysubs;
        sublist *subber, *tmp;

        HASH_FIND_STR(sstate.subs, s->key, keysubs);

        if (keysubs) {
            HASH_ITER(hh, keysubs->subs, subber, tmp) {
                send = zframe_dup(template);
                zframe_send(subber->client_socket, send, 0);
            }
        }
    }

    zframe_destroy(&template);
}

void handle_set_message(unsigned char * data, size_t len) {
    if (sstate.role != ROLE_MASTER)
        return; // only the master can handle set messages

    size_t off;

    const char *host = NULL;
    const char *key = NULL;
    const char *lock = NULL;
    msgpack_object_raw *value = NULL;


    msgpack_unpacked msg;

    MSG_NEXT(&msg, data, len, &off);
    assert(msg.data.type == MSGPACK_OBJECT_RAW);
    host = msg.data.via.raw.ptr;
    (void)host; // unused for now

    MSG_NEXT(&msg, data, len, &off);
    assert(msg.data.type == MSGPACK_OBJECT_RAW);
    key = msg.data.via.raw.ptr;

    MSG_NEXT(&msg, data, len, &off);
    switch (msg.data.type) {
        case MSGPACK_OBJECT_RAW:
            lock = msg.data.via.raw.ptr;
            break;
        case MSGPACK_OBJECT_NIL:
            lock = NULL;
            break;
        default:
            assert(0);
    }

    MSG_NEXT(&msg, data, len, &off);
    assert(msg.data.type == MSGPACK_OBJECT_POSITIVE_INTEGER);
    uint64_t limit = msg.data.via.u64;

    MSG_NEXT(&msg, data, len, &off);
    assert(msg.data.type == MSGPACK_OBJECT_RAW);
    value = &msg.data.via.raw;

    stored_set *set;
    stored_value *new;

    HASH_FIND_STR(sstate.db, key, set);

    if (set) {
        if (!limit || set->num_values < limit) {
            txid++;
            new = (stored_value *)malloc(sizeof(stored_value));
            if (lock) {
                char *llock = malloc(strlen(lock));
                strcpy(llock, lock);
                new->lock = llock;
            }
            else {
                new->lock = NULL;
            }

            new->data.ptr = malloc(value->size);
            memcpy((void *)new->data.ptr, value->ptr, value->size);
            new->data.size = value->size;

            set->num_values++;
            set->txid = txid;
            LL_APPEND(set->values, new);

            fire_update(set, NULL);
        }

    }
    else {
        txid++;
        set = (stored_set *)malloc(sizeof(stored_set));
        char *lkey = malloc(strlen(key));
        strcpy(lkey, key);
        set->key = lkey;
        set->values = NULL;

        new = (stored_value *)malloc(sizeof(stored_value));
        if (lock) {
            char *llock = malloc(strlen(lock));
            strcpy(llock, lock);
            new->lock = llock;
        }
        else {
            new->lock = NULL;
        }
        new->data.ptr = malloc(value->size);
        memcpy((void *)new->data.ptr, value->ptr, value->size);
        new->data.size = value->size;

        set->num_values++;
        set->txid = txid;
        LL_APPEND(set->values, new);

        fire_update(set, NULL);
    }
}

typedef struct {
    char *key;
    unsigned long long txid;
} follow_pair;

void handle_follow_message(void * data, size_t len) {
    int person_added = 0;

    const char *host = NULL;
    uint64_t txid; 
    msgpack_unpacked msg;
    size_t off;
    const char *key;

    MSG_NEXT(&msg, data, len, &off);
    assert(msg.data.type == MSGPACK_OBJECT_RAW);
    host = msg.data.via.raw.ptr;

    key_sub *subkey;
    sublist *subber = check_subber_indexed(host, &person_added);

    MSG_NEXT(&msg, data, len, &off);
    assert(msg.data.type == MSGPACK_OBJECT_ARRAY);
    int count = msg.data.via.array.size;

    int i = 0;

    for (i=0; i < count; i++) {
        /* for each subscription, make sure it's added
         * and broadcast an update if it's out of sync */
        msgpack_object * obj = &msg.data.via.array.ptr[i];
        assert(obj->type == MSGPACK_OBJECT_RAW);
        key = obj->via.raw.ptr;
        i++;

        obj = &msg.data.via.array.ptr[i];
        assert(obj->type == MSGPACK_OBJECT_POSITIVE_INTEGER);
        txid = obj->via.u64;

        HASH_FIND_STR(sstate.subs, key, subkey);

        if (!subkey) {
            subkey = (key_sub *)malloc(sizeof(key_sub));
            subkey->key = malloc(strlen(key));
            strcpy(subkey->key, key);
            subkey->subs = NULL;
        }

        sublist *app;
        HASH_FIND_STR(subkey->subs, host, app);
        if (!app) {
            /* need a copy for the HT */
            app = (sublist *)malloc(sizeof(sublist));

            /* NOTE -- pointer to host char * and 
             * socket char * is copied here */
            memcpy(app, subber, sizeof(sublist));
            HASH_ADD_STR(subkey->subs, host, app);
        }

        stored_set *ss;
        HASH_FIND_STR(sstate.db, key, ss);
        if (ss && ss->txid != txid)
            fire_update(ss, subber->client_socket);
    }
}

void start_master(zctx_t *ctx, consensus_host *slaves, int num_slaves) {
    int i;
    sstate.role = ROLE_MASTER;
    sstate.ctx = ctx;
    sstate.slaves = NULL;
    peer_map *map;

    for (i=0; i < num_slaves; i++) {
        map = (peer_map *)malloc(sizeof(peer_map));
        map->host = slaves[i].host;
        map->peer_socket = slaves[i].peer_socket;
        HASH_ADD_STR(sstate.slaves, host, map);

    }
}

void end_master(zctx_t *ctx) {
    sstate.role = ROLE_ELECTING;
    sstate.slaves = NULL;
}
