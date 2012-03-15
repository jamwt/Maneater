#include <czmq.h>

#include "maneater.h"
#include "uthash.h"
#include "utlist.h"

typedef struct stored_value {
    char *lock;
    maneater_bin data;

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
    char *lock;
    char *key;

    UT_hash_handle hh;
} lock_sub;

typedef struct {
    ROLE role;
    zctx_t *ctx;
    peer_map *slaves;
    int num_slaves;
    stored_set *db;
    key_sub *subs;
    sublist *hostsockets;
    sublist *locksets;
    void * master_socket;
} set_state;

static unsigned long long txid;
static set_state sstate;


void set_init() {
    txid = 0;
    sstate.role = ROLE_ELECTING;
    sstate.slaves = NULL;
    sstate.num_slaves = 0;
    sstate.db = NULL;
    sstate.hostsockets = NULL;
    sstate.locksets = NULL;
    sstate.subs = NULL;
}

sublist * check_subber_indexed(const char *host, int * added) {
    sublist *subber = NULL;
    HASH_FIND_STR(sstate.hostsockets, host, subber);

    *added = 0;
    if (!subber) {
        subber = (sublist *)malloc(sizeof (sublist));
        COPY_STRING(subber->host, host);
        *added = 1;

        subber->client_socket = zsocket_new(sstate.ctx, ZMQ_DEALER);
        zsocket_connect(subber->client_socket, "tcp://%s", host);
        MHASH_STRING_SET(sstate.hostsockets, host, subber);
        HASH_FIND_STR(sstate.hostsockets, host, subber);
        assert(subber);
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
        printf("values? %d\n", s->num_values);

        stored_value *val = NULL;
        for (val = s->values; val; val = val->next) {
            printf("packing.. %d %s\n", val->data.size, (char*)val->data.ptr);
            msgpack_pack_raw(pk, val->data.size);
            msgpack_pack_raw_body(pk,
                val->data.ptr,
                val->data.size);
        }

        template = zframe_new(buf->data, buf->size);
    );

    if (msock) {
        send = zframe_dup(template);
        zframe_send(&send, msock, 0);
    }
    else {
        if (sstate.role == ROLE_MASTER) {
            for (i=0; i < sstate.num_slaves; i++) {
                send = zframe_dup(template);
                zframe_send(&send, sstate.slaves[i].peer_socket, 0);
            }
        }

        key_sub *keysubs;
        sublist *subber, *tmp;

        HASH_FIND_STR(sstate.subs, s->key, keysubs);

        if (keysubs) {
            HASH_ITER(hh, keysubs->subs, subber, tmp) {
                send = zframe_dup(template);
                zframe_send(&send, subber->client_socket, 0);
            }
        }
    }

    zframe_destroy(&template);
}

void handle_get_message(unsigned char * data, size_t len) {
    size_t off;

    const char *host = NULL;
    const char *key = NULL;

    msgpack_unpacked msg;

    MSG_NEXT(&msg, data, len, &off);
    assert(msg.data.type == MSGPACK_OBJECT_RAW);
    host = msg.data.via.raw.ptr;

    MSG_NEXT(&msg, data, len, &off);
    assert(msg.data.type == MSGPACK_OBJECT_RAW);
    key = msg.data.via.raw.ptr;

    int l = strlen(key);
    int person_added;
    stored_set *s, tmp;
    sublist *subber = check_subber_indexed(host, &person_added);
    assert(subber);

    int nl = l - 1;
    if (key[nl] == '*') {
        char *tkey;
        COPY_STRING(tkey, key);
        tkey[nl] = 0;

        for (s=sstate.db; s; s = s->hh.next) {
            /* NOTE: nl == 0 is '*' case */
            if (strlen(s->key) >= nl
                && (nl == 0 || !strncmp(s->key, tkey, nl))) {
                fire_update(s, subber->client_socket);
            }
        }

        free(tkey);
    }
    else {
        HASH_FIND_STR(sstate.db, key, s);
        if (!s) {
            tmp.key = (char *)key;
            tmp.values = NULL;
            tmp.txid = 0;
            tmp.num_values = 0;
            s = &tmp;
        }

        fire_update(s, subber->client_socket);
    }
}

void do_delete(const char *key, maneater_bin value) {
    stored_set *s;
    stored_value *v, *tmp;

    HASH_FIND_STR(sstate.db, key, s);
    if (s) {
        txid++;
        LL_FOREACH_SAFE(s->values, v, tmp) {
            if (!value.ptr ||
            BINARIES_EQUAL(value, v->data)) {
                LL_DELETE(s->values, v);
                free(v->data.ptr);
                free(v);
                s->num_values--;
            }
        }
        s->txid = txid;
        fire_update(s, NULL);
    }
}


void handle_del_message(unsigned char * data, size_t len) {
    size_t off;

    const char *key = NULL;
    maneater_bin value;

    msgpack_unpacked msg;

    MSG_NEXT(&msg, data, len, &off);
    assert(msg.data.type == MSGPACK_OBJECT_RAW);
    key = msg.data.via.raw.ptr;

    MSG_NEXT(&msg, data, len, &off);
    switch (msg.data.type) {
        case MSGPACK_OBJECT_RAW: 
            value.ptr = (void *) msg.data.via.raw.ptr;
            value.size =  msg.data.via.raw.size;
            break;
        case MSGPACK_OBJECT_NIL: 
            value.ptr = NULL;
            value.size = 0;
            break;
        default: assert(0); /* unknown type next */
    }
    do_delete(key, value);
}

void handle_set_message(unsigned char * data, size_t len) {
    if (sstate.role != ROLE_MASTER)
        return; // only the master can handle set messages

    size_t off;

    const char *host = NULL;
    const char *key = NULL;
    const char *lock = NULL;
    maneater_bin value;

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
    value.ptr = (void *)msg.data.via.raw.ptr;
    value.size = msg.data.via.raw.size;

    stored_set *set;
    stored_value *new;

    HASH_FIND_STR(sstate.db, key, set);

    if (set) {
        if (!limit || set->num_values < limit) {
            for (new = set->values; new; new = new->next) {
                if (BINARIES_EQUAL(new->data, value))
                    return; /* set!  don't duplicate */
            }
            txid++;
            new = (stored_value *)malloc(sizeof(stored_value));
            if (lock) {
                COPY_STRING(new->lock, lock);
            }
            else {
                new->lock = NULL;
            }

            COPY_MEM(new->data.ptr, value.ptr, value.size);
            new->data.size = value.size;

            set->num_values++;
            set->txid = txid;
            LL_APPEND(set->values, new);

            fire_update(set, NULL);
        }

    }
    else {
        txid++;
        set = (stored_set *)malloc(sizeof(stored_set));
        COPY_STRING(set->key, key);
        set->values = NULL;

        new = (stored_value *)malloc(sizeof(stored_value));
        if (lock) {
            COPY_STRING(new->lock, lock);
        }
        else {
            new->lock = NULL;
        }
        COPY_MEM(new->data.ptr, value.ptr, value.size);
        new->data.size = value.size;

        set->num_values = 1;
        set->txid = txid;
        LL_APPEND(set->values, new);

        MHASH_STRING_SET(sstate.db, key, set);

        fire_update(set, NULL);
    }
}

typedef struct {
    char *key;
    unsigned long long txid;
} follow_pair;

void handle_follow_message(unsigned char * data, size_t len) {
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

        printf("doing for %s %llu\n", key, (long long unsigned)txid);

        HASH_FIND_STR(sstate.subs, key, subkey);

        if (!subkey) {
            subkey = (key_sub *)malloc(sizeof(key_sub));
            COPY_STRING(subkey->key, key);
            subkey->subs = NULL;

            MHASH_STRING_SET(sstate.subs, key, subkey);
        }

        sublist *app;
        HASH_FIND_STR(subkey->subs, host, app);
        if (!app) {
            /* need a copy for the HT */
            app = (sublist *)malloc(sizeof(sublist));

            /* NOTE -- pointer to host char * and 
             * socket char * is copied here */
            memcpy(app, subber, sizeof(sublist));
            MHASH_STRING_SET(subkey->subs, host, app);
        }

        stored_set *ss;
        HASH_FIND_STR(sstate.db, key, ss);
        if (!ss && txid == START_TXID) {
            stored_set s;
            s.key = (char *)key;
            s.values = NULL;
            s.txid = 0;
            s.num_values = 0;
            fire_update(&s, subber->client_socket);
        }
        else if (ss && ss->txid != txid)
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
        MHASH_STRING_SET(sstate.slaves, host, map);
    }
}

void end_master(zctx_t *ctx) {
    sstate.role = ROLE_ELECTING;
    sstate.ctx = NULL;

    peer_map *map, *tmp;
    HASH_ITER(hh, sstate.slaves, map, tmp) {
        HASH_DEL(sstate.slaves, map);
        free(map);
    }
    sstate.slaves = NULL;
}

void start_slave(zctx_t *ctx, void *master_socket) {
    sstate.role = ROLE_SLAVE;
    sstate.ctx = ctx;
    sstate.master_socket = master_socket;
}

void end_slave(zctx_t *ctx) {
    sstate.role = ROLE_ELECTING;
    sstate.ctx = NULL;
    sstate.master_socket = NULL;
}
