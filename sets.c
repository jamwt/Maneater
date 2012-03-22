#include <czmq.h>

#include "maneater.h"
#include "uthash.h"
#include "utlist.h"

/* Represents the actual stored
 * value in the database; it's a LL
 * b/c it's a series of items;
 * optionally, the stored_value is
 * tied to a specific client session
 * which may timeout
 */
typedef struct stored_value {
    void *owner; /* sessionid or NULL */
    maneater_bin data;

    /* for utlist.h */
    struct stored_value *prev;
    struct stored_value *next;
} stored_value;

/* Represents the set of stored items
 * for a given key in the db
 */
typedef struct {
    char * key;
    stored_value * values;
    unsigned long long txid;
    int num_values;
    UT_hash_handle hh;
} stored_set;

/* Hostname to socket map for slaves (NOT clients)
 */
typedef struct {
    char *host;
    void *peer_socket;
    UT_hash_handle hh;
} peer_map;

/* A key (within the list of keys) that a particular
 * client/session is subscribed to*/
typedef struct keyset {
    char *key;

    UT_hash_handle hh;
} keyset;

/* A session (that represents a running instance of
 * a client); includes the socket to that client as
 * well as the list of keys to which this client is
 * subscribed.  The last timestamp for ALIVE is tracked
 * as well for session timeout purposes. */
typedef struct {
    char *sessionid;
    uint32_t tick;
    void *client_socket;

    keyset * subs;
    keyset * owns;
    UT_hash_handle hh;
} session;

/* The list of subscribing sessions for a db _KEY_ */
typedef struct {
    char *key;
    session *sessions;
    UT_hash_handle hh;
} key_sub;

/* The state of the entire set database */
typedef struct {
    ROLE role;
    zctx_t *ctx;
    peer_map *slaves;
    int num_slaves;
    stored_set *db;
    key_sub *subs;
    session *sessions;
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

    /* key -> sessions which subscribe */
    sstate.subs = NULL;

    /* session id -> session */
    sstate.sessions = NULL;
}

/* forward declaration, defined lower in file */
void do_delete(const char *key, maneater_bin value, session *owner);

void touch_session(const char *sessionid) {
    struct timeval tim;
    gettimeofday(&tim, NULL);

    session *ses;
    HASH_FIND_STR(sstate.sessions, sessionid, ses);
    if (ses)
        ses->tick = tim.tv_sec;
}

int check_expired_sessions(zloop_t *loop, zmq_pollitem_t *item, void *arg) {
    if (sstate.role != ROLE_MASTER)
        return 0;
    session *ses, *tmp, *copy;
    struct timeval tim;
    gettimeofday(&tim, NULL);
    maneater_bin empty;
    empty.ptr = NULL;

    HASH_ITER(hh, sstate.sessions, ses, tmp) {
        zclock_log("checking...");
        if (tim.tv_sec - ses->tick >= SESSION_TIMEOUT) {
            zclock_log("timeout! %s\n", ses->sessionid);
            keyset *ks, *tmp;

            /* 1. Release locks */
            HASH_ITER(hh, ses->owns, ks, tmp) {
                do_delete(ks->key, empty, ses);
                HASH_DEL(ses->owns, ks);

                free(ks->key);
                free(ks);
            }

            /* 2. Remove all subs */
            HASH_ITER(hh, ses->subs, ks, tmp) {
                key_sub *sub = NULL;
                HASH_FIND_STR(sstate.subs, ks->key, sub);
                if (sub) {
                    HASH_FIND_STR(sub->sessions, ses->sessionid, copy);
                    if (copy) {
                        HASH_DEL(sub->sessions, copy);
                        free(copy);
                    }
                }

                HASH_DEL(ses->subs, ks);

                free(ks->key);
                free(ks);
            }


            /* 3. Close down socket */
            zsocket_destroy(sstate.ctx, ses->client_socket);
            
            /* 4. Remove from hash */
            HASH_DEL(sstate.sessions, ses);

            /* 5. Free session */
            free(ses->sessionid);
            free(ses);
        }
    }
    return 0;
}


session * check_session_indexed(const char *sessionid,
        const char *host) {
    session *ses = NULL;
    HASH_FIND_STR(sstate.sessions, sessionid, ses);

    if (!ses) {
        ses = (session *)malloc(sizeof (session));
        COPY_STRING(ses->sessionid, sessionid);
        ses->subs = NULL;
        ses->owns = NULL;

        struct timeval tim;
        gettimeofday(&tim, NULL);
        ses->tick = tim.tv_sec;

        ses->client_socket = zsocket_new(sstate.ctx, ZMQ_DEALER);
        zsocket_connect(ses->client_socket, "tcp://%s", host);
        MHASH_STRING_SET(sstate.sessions, sessionid, ses);
        HASH_FIND_STR(sstate.sessions, sessionid, ses);
        assert(ses);
    }

    return ses;
}


void send_to_session(const char *sessionid, 
        const char *host, zframe_t *frame) {
   session * ses = check_session_indexed(sessionid, 
           host);

   assert(ses);

   zframe_send(&frame, ses->client_socket, 0);
}

void fire_update(stored_set *s, void *msock) {
    zframe_t *template, *send;
    int i;

    MSG_DOPACK(
        msgpack_pack_int(pk, MID_VALUE);
        MSG_PACK_STR(pk, s->key);
        msgpack_pack_uint64(pk, s->txid);
        msgpack_pack_array(pk, s->num_values);

        stored_value *val = NULL;
        for (val = s->values; val; val = val->next) {
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
        session *ses, *tmp;

        HASH_FIND_STR(sstate.subs, s->key, keysubs);

        if (keysubs) {
            HASH_ITER(hh, keysubs->sessions, ses, tmp) {
                send = zframe_dup(template);
                zframe_send(&send, ses->client_socket, 0);
            }
        }
    }

    zframe_destroy(&template);
}

void handle_get_message(unsigned char * data, size_t len) {
    size_t off;

    const char *sessionid = NULL;
    const char *host = NULL;
    const char *key = NULL;

    msgpack_unpacked msg;

    MSG_NEXT(&msg, data, len, &off);
    assert(msg.data.type == MSGPACK_OBJECT_RAW);
    sessionid = msg.data.via.raw.ptr;

    MSG_NEXT(&msg, data, len, &off);
    assert(msg.data.type == MSGPACK_OBJECT_RAW);
    host = msg.data.via.raw.ptr;

    MSG_NEXT(&msg, data, len, &off);
    assert(msg.data.type == MSGPACK_OBJECT_RAW);
    key = msg.data.via.raw.ptr;

    int l = strlen(key);
    stored_set *s, tmp;
    session *ses = check_session_indexed(sessionid, host);
    assert(ses);

    int nl = l - 1;
    if (key[nl] == '*') {
        char *tkey;
        COPY_STRING(tkey, key);
        tkey[nl] = 0;

        for (s=sstate.db; s; s = s->hh.next) {
            /* NOTE: nl == 0 is '*' case */
            if (strlen(s->key) >= nl
                && (nl == 0 || !strncmp(s->key, tkey, nl))) {
                fire_update(s, ses->client_socket);
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

        fire_update(s, ses->client_socket);
    }
}

void do_delete(const char *key, maneater_bin value, session *owner) {
    stored_set *s;
    stored_value *v, *tmp;

    HASH_FIND_STR(sstate.db, key, s);
    if (s) {
        txid++;
        LL_FOREACH_SAFE(s->values, v, tmp) {
            if ((owner && owner == (session *)v->owner)
               || (!value.ptr || BINARIES_EQUAL(value, v->data))) {
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
    do_delete(key, value, NULL);
}

void handle_set_message(unsigned char * data, size_t len) {
    if (sstate.role != ROLE_MASTER)
        return; // only the master can handle set messages

    size_t off;

    const char *host = NULL;
    const char *key = NULL;
    const char *sessionid = NULL;

    maneater_bin value;

    msgpack_unpacked msg;

    MSG_NEXT(&msg, data, len, &off);
    assert(msg.data.type == MSGPACK_OBJECT_RAW);
    host = msg.data.via.raw.ptr;
    session * ses = NULL;

    MSG_NEXT(&msg, data, len, &off);
    assert(msg.data.type == MSGPACK_OBJECT_RAW);
    key = msg.data.via.raw.ptr;

    MSG_NEXT(&msg, data, len, &off);
    switch (msg.data.type) {
        case MSGPACK_OBJECT_RAW:
            sessionid = msg.data.via.raw.ptr;
            ses = check_session_indexed(sessionid, host);
            break;
        case MSGPACK_OBJECT_NIL:
            sessionid = NULL;
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

    if (!set) {
        set = (stored_set *)malloc(sizeof(stored_set));
        COPY_STRING(set->key, key);
        set->values = NULL;
        set->num_values = 0;
        MHASH_STRING_SET(sstate.db, key, set);
        set = NULL;
        HASH_FIND_STR(sstate.db, key, set);
        assert(set);
    }

    if (!limit || set->num_values < limit) {
        for (new = set->values; new; new = new->next) {
            if (BINARIES_EQUAL(new->data, value))
                return; /* set!  don't duplicate */
        }
        txid++;
        new = (stored_value *)malloc(sizeof(stored_value));
        new->owner = ses;

        COPY_MEM(new->data.ptr, value.ptr, value.size);
        new->data.size = value.size;

        set->num_values++;
        set->txid = txid;
        LL_APPEND(set->values, new);

        if (ses) {
            keyset *own = NULL;
            HASH_FIND_STR(ses->owns, key, own);
            if (!own) {
                own = (keyset *)malloc(sizeof(keyset));
                COPY_STRING(own->key, key);
                MHASH_STRING_SET(ses->owns, key, own);
                own = NULL;
                HASH_FIND_STR(ses->owns, key, own);
                assert(own);
            }
        }

        fire_update(set, NULL);
    }

}

void handle_follow_message(unsigned char * data, size_t len) {

    const char *host = NULL;
    const char *sessionid = NULL;
    uint64_t txid;
    msgpack_unpacked msg;
    size_t off;
    const char *key;

    MSG_NEXT(&msg, data, len, &off);
    assert(msg.data.type == MSGPACK_OBJECT_RAW);
    sessionid = msg.data.via.raw.ptr;

    MSG_NEXT(&msg, data, len, &off);
    assert(msg.data.type == MSGPACK_OBJECT_RAW);
    host = msg.data.via.raw.ptr;

    key_sub *subkey;
    session *ses = check_session_indexed(sessionid, host);

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
            COPY_STRING(subkey->key, key);
            subkey->sessions = NULL;

            MHASH_STRING_SET(sstate.subs, key, subkey);
        }

        session *app;
        HASH_FIND_STR(subkey->sessions, sessionid, app);
        if (!app) {
            /* need a copy for the HT
             * different hashtable than the
             * sessionid-keyed one */
            app = (session *)malloc(sizeof(session));

            /* NOTE -- pointer to host char * and 
             * socket char * is copied here */
            memcpy(app, ses, sizeof(session));
            MHASH_STRING_SET(subkey->sessions, sessionid, app);
        }

        stored_set *ss;
        HASH_FIND_STR(sstate.db, key, ss);
        if (!ss && txid == START_TXID) {
            stored_set s;
            s.key = (char *)key;
            s.values = NULL;
            s.txid = 0;
            s.num_values = 0;
            fire_update(&s, ses->client_socket);
        }
        else if (ss && ss->txid != txid)
            fire_update(ss, ses->client_socket);

        keyset *find;
        HASH_FIND_STR(ses->subs, key, find);
        if (!find) {
            find = (keyset *)malloc(sizeof(keyset));
            COPY_STRING(find->key, key);
            MHASH_STRING_SET(ses->subs, key, find);
            find = NULL;
            HASH_FIND_STR(ses->subs, key, find);
            assert(find);
        }
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
