#include <czmq.h>

#include "maneater.h"
#include "uthash.h"
#include "utlist.h"
#include "tpl.h"

typedef struct {
    char *v;
    int l;
} bin;

typedef struct stored_value {
    char *lock;
    bin data;

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

void fire_update(stored_set *s, void *msock) {
    zframe_t *template, *send;
    tpl_node *tn;
    tpl_bin tb;
    int i, size;
    void *dumped;

    tn = tpl_map(TPL_VALUE_MAP, s->key, s->txid, &tb);

    tpl_pack(tn, 0);

    for (i=0; i < s->num_values; i++) {
        tb.addr = s->values[i].data.v;
        tb.sz = s->values[i].data.l;
        tpl_pack(tn, 1);
    }

    tpl_dump(tn, TPL_GETSIZE, &size);
    dumped = (void *)malloc(size + 1);
    ((char *)dumped)[0] = MID_VALUE;

    tpl_dump(tn, TPL_MEM|TPL_PREALLOCD, dumped + 1, size);

    tpl_free(tn);

    template = zframe_new(dumped, size + 1);

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

    free(dumped);
    zframe_destroy(&template);
}

void handle_set_message(void * msg, size_t len) {
    if (sstate.role != ROLE_MASTER)
        return; // only the master can handle set messages

    tpl_node * tn;
    char *host = NULL;
    char *key = NULL;
    char *lock = NULL;
    unsigned short limit;
    tpl_bin tb;

    tn = tpl_map(TPL_SET_MAP, host, key, lock, &limit, &tb);
    tpl_load(tn, TPL_MEM, msg, len);
    tpl_unpack(tn, 0);

    stored_set *set;
    stored_value *new;

    HASH_FIND_STR(sstate.db, key, set);

    if (set) {
        if (limit && set->num_values >= limit) {
            goto all_unused;
        }
        else {
            txid++;
            new = (stored_value *)malloc(sizeof(stored_value));
            new->lock = lock;
            new->data.v = tb.addr;
            new->data.l = tb.sz;

            set->num_values++;
            set->txid = txid;
            LL_APPEND(set->values, new);

            fire_update(set, NULL);

            goto key_unused;
        }

    }
    else {
        txid++;
        set = (stored_set *)malloc(sizeof(stored_set));
        set->key = key;
        set->values = NULL;

        new = (stored_value *)malloc(sizeof(stored_value));
        new->lock = lock;
        new->data.v = tb.addr;
        new->data.l = tb.sz;

        set->num_values++;
        set->txid = txid;
        LL_APPEND(set->values, new);

        fire_update(set, NULL);

        goto final_free;
    }

all_unused:
    if (lock)
        free(lock);

key_unused:
    free(key);

final_free:

    free(host);
    tpl_free(tn);
}

typedef struct {
    char *key;
    unsigned long long txid;
} follow_pair;

void handle_follow_message(void * msg, size_t len) {
    tpl_node * tn;
    char connbuf[150] = {0};
    int used_host = 0;

    char *host = NULL;
    follow_pair pair;

    tn = tpl_map(TPL_FOLLOW_MAP, host, &pair);
    tpl_load(tn, TPL_MEM, msg, len);
    tpl_unpack(tn, 0);
    
    key_sub *subkey;
    sublist *subber;

    HASH_FIND_STR(sstate.hostsockets, host, subber);
    
    if (!subber) {
        subber = (sublist *)malloc(sizeof (sublist));
        subber->host = host;
        used_host = 1;

        subber->client_socket = zsocket_new(sstate.ctx, ZMQ_DEALER);
        snprintf(connbuf, sizeof(connbuf), "tcp://%s", host);
        zsocket_connect(subber->client_socket, connbuf);
        HASH_ADD_STR(sstate.hostsockets, host, subber);
    }

    while (tpl_unpack(tn, 1) > 0) {
        /* for each subscription, make sure it's added
         * and broadcast an update if it's out of sync */
        HASH_FIND_STR(sstate.subs, pair.key, subkey);

        if (!subkey) {
            subkey = (key_sub *)malloc(sizeof(key_sub));
            subkey->key = pair.key;
            subkey->subs = NULL;
        }
        else {
            free(pair.key); // don't need this copy of the key
        }

        sublist *app;
        HASH_FIND_STR(subkey->subs, host, app);
        if (!app) {
            /* need a copy for the HT */
            app = (sublist *)malloc(sizeof(sublist));
            memcpy(app, subber, sizeof(sublist));
            HASH_ADD_STR(subkey->subs, host, app);
        }

        stored_set *ss;
        HASH_FIND_STR(sstate.db, pair.key, ss);
        if (ss && ss->txid != pair.txid)
            fire_update(ss, subber->client_socket);
    }

    if (!used_host)
        free(host);
    tpl_free(tn);
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
