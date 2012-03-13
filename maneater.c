#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <czmq.h>
#include <sys/time.h>

#include "maneater.h"
#include "uuid.h"
#include "uthash.h"

typedef struct {
    char nodeid[NODEID_LEN];
    UT_hash_handle hh;
} my_advocate;

#define ELECTION_RESET_TIMEOUT 5
typedef struct {
    char * master;
    char advocating[NODEID_LEN];
    unsigned int reset_timer;
    ROLE role;
    my_advocate *myads;
} consensus_state;

enum {
    MID_ADVOCATE,
    MID_OBEY
};

static char myid[NODEID_LEN];
static int num_hosts;
static consensus_host * g_hosts;
static zctx_t * ctx;
#define ERRBUF_SZ 100
static char errbuf[ERRBUF_SZ];
void *local_socket;
static zloop_t * mainloop;
static consensus_state cstate;

void error_and_fail(char *err) {
    fprintf(stderr, "error: %s\n", err);
    exit(1);
}

void handle_args(int argc, char **argv) {
    int i;
    num_hosts = argc - 1;
    if (num_hosts == 0)
        error_and_fail("please provide at least 1 consensus set member");

    if (num_hosts % 2 == 0)
        error_and_fail("please provide an odd number of consensus set members");

    g_hosts = (consensus_host *)malloc(num_hosts * sizeof(consensus_host));

    for (i=0; i < num_hosts; i++) {
        char * h = argv[i + 1];
        if (strlen(h) >= HOSTID_LEN - 10)
            error_and_fail("host identifier too long");
        g_hosts[i].host = h;
    }
}

void restart_election() {
    zclock_log("{election_runner} (RE)START");
    cstate.reset_timer = 0xffffffff;
    if (cstate.master) {
        free(cstate.master);
        cstate.master = NULL;
    }

    if (cstate.role == ROLE_MASTER) {
        end_master(ctx);
    }
    else if (cstate.role == ROLE_SLAVE) {
        end_slave(ctx);
    }
    strcpy(cstate.advocating, myid);
    cstate.role = ROLE_ELECTING;

    my_advocate *ad, *tmp;
    HASH_ITER(hh, cstate.myads, ad, tmp) {
        HASH_DEL(cstate.myads, ad);
        free(ad);
    }
}

void setup_zeromq() {
    int i, conn_succ;
    ctx = zctx_new();

    /* setup local bind */
    consensus_host *h = &(g_hosts[0]); // convention: I am the first
    local_socket = zsocket_new(ctx, ZMQ_DEALER);

    /* Note: no checking for error here b/c czmq will assert on invalid bind */
    conn_succ = zsocket_bind(local_socket, "tcp://%s", h->host);

    /* setup peer connections (including to myself) */
    for (i=0; i < num_hosts; i++) {
        h = g_hosts + i;
        h->peer_socket = zsocket_new(ctx, ZMQ_DEALER);
        conn_succ = zsocket_connect(h->peer_socket, "tcp://%s", h->host);
        if (conn_succ != 0) {
            snprintf(errbuf, ERRBUF_SZ - 1, "invalid peer specification (%s)", h->host);
            error_and_fail(errbuf);
        }
    }
}

void setup_node_state() {
    init_random_system();

    struct timeval tim;
    gettimeofday(&tim, NULL);

    int l = snprintf(myid, 9, "%08x", (unsigned int)tim.tv_sec);
    generate_uuid4(myid + l);

    cstate.myads = NULL;
    restart_election();
}

void test_for_master() {
    int quorum_amt = (num_hosts >> 1) + 1;
    if (HASH_COUNT(cstate.myads) >= quorum_amt) {
        zclock_log("MASTER");
        cstate.role = ROLE_MASTER;
        consensus_host * hosts;
        cstate.master = (char *)malloc(strlen(g_hosts[0].host) + 1);
        strcpy(cstate.master, g_hosts[0].host);
        if (num_hosts == 1)
            hosts = NULL;
        else
            hosts = &(g_hosts[1]);

        start_master(ctx, hosts, num_hosts - 1);
    }
}

void handle_advocate(unsigned char* data, int size) {
    msgpack_unpacked msg;
    size_t off;

    MSG_NEXT(&msg, data, size, &off);
    msgpack_object_print(stderr, msg.data);
    assert(msg.data.type == MSGPACK_OBJECT_RAW);
    char *nodeid = (char *)msg.data.via.raw.ptr;

    MSG_NEXT(&msg, data, size, &off);
    msgpack_object_print(stderr, msg.data);
    assert(msg.data.type == MSGPACK_OBJECT_RAW);
    char *advocating = (char *)msg.data.via.raw.ptr;

    // we haven't given up on the master yet?
    if (cstate.role != ROLE_ELECTING)
        return;

    zclock_log("got advocation: %s vs. %s", advocating);

    /* 1. If this is LOWER than my current advocating value
     * I advocate for it now */
    if (strcmp(advocating, cstate.advocating) < 0) {
        zclock_log("changing advocation: %s -> %s", cstate.advocating, advocating);
        strcpy(cstate.advocating, advocating);
        struct timeval tim;
        gettimeofday(&tim, NULL);
        cstate.reset_timer = tim.tv_sec + ELECTION_RESET_TIMEOUT;
    }

    /* 2. If this node is me, then add this to my set
     * of advocates; if I get a quorum, declare myself
     * master */

    my_advocate * fad;

    if (! strcmp(advocating, myid) ) {
        HASH_FIND_STR(cstate.myads, nodeid, fad);
        if (! fad) {
            zclock_log("new advocate for me: %s", nodeid);
            fad = malloc(sizeof(my_advocate));
            strcpy(fad->nodeid, nodeid);
            HASH_ADD_STR(cstate.myads, nodeid, fad);
            test_for_master();
        }
    }
}

void handle_obey(unsigned char *data, int size) {
    msgpack_unpacked msg;
    msgpack_unpacked_init(&msg);
    size_t off;

    MSG_NEXT(&msg, data, size, &off);
    assert(msg.data.type == MSGPACK_OBJECT_RAW);
    char *host = (char *)msg.data.via.raw.ptr;

    struct timeval tim;
    gettimeofday(&tim, NULL);
    cstate.reset_timer = tim.tv_sec + ELECTION_RESET_TIMEOUT;

    if (cstate.role == ROLE_ELECTING) {
        cstate.role = ROLE_SLAVE;
        cstate.master = (char *)malloc(strlen(host) + 1);
        strcpy(cstate.master, host);
        zclock_log("SLAVE (master = %s)", cstate.master);
        int i;
        void * master_socket = NULL;
        for (i=1; i < num_hosts; i++) {
            if (!strcmp(g_hosts[i].host, cstate.master)) {
                    master_socket = g_hosts[i].peer_socket;
                    break;
            }
        }

        assert(master_socket);

        start_slave(ctx, master_socket);
    }
}

void handle_client_alive(unsigned char *data, int len) {
    msgpack_unpacked msg;
    size_t off;
    zframe_t *out;

    if (cstate.role == ROLE_MASTER) {
        MSG_NEXT(&msg, data, len, &off);
        assert(msg.data.type == MSGPACK_OBJECT_RAW);
        const char *rethost = msg.data.via.raw.ptr;

        MSG_NEXT(&msg, data, len, &off);
        assert(msg.data.type == MSGPACK_OBJECT_RAW);
        const char *sessid = msg.data.via.raw.ptr;
        /* XXX call into sets to renew sessid locks */
        (void)sessid;

        MSG_DOPACK(
            msgpack_pack_int(pk, MID_S_ALIVE);

            out = zframe_new(buf->data, buf->size);
        );

        send_to_host(rethost, out);
    }
}

void handle_want_master(unsigned char *data, int len) {
    msgpack_unpacked msg;
    size_t off;

    MSG_NEXT(&msg, data, len, &off);
    assert(msg.data.type == MSGPACK_OBJECT_RAW);
    const char *host = msg.data.via.raw.ptr;
    zframe_t *out;

    if (cstate.role != ROLE_ELECTING) {
        MSG_DOPACK(
            msgpack_pack_int(pk, MID_IS_MASTER);
            MSG_PACK_STR(pk, cstate.master);

            out = zframe_new(buf->data, buf->size);
        );

        send_to_host(host, out);
    }
}

int input_event(zloop_t *loop, zmq_pollitem_t *item, void *arg) {

    zframe_t *incoming = zframe_recv(local_socket);
    unsigned char *data = zframe_data(incoming);
    int size = zframe_size(incoming);
    size_t off;

    msgpack_unpacked msg;
    msgpack_unpacked_init(&msg);
    MSG_NEXT(&msg, data, size, &off);
    assert(msg.data.type == MSGPACK_OBJECT_POSITIVE_INTEGER);
    uint64_t msgid = msg.data.via.u64;

    switch (msgid) {
        case MID_ADVOCATE:
            handle_advocate(data, size);
            break;
        case MID_OBEY:
            handle_obey(data, size);
            break;
        case MID_SET:
            handle_set_message(data, size);
            break;
        case MID_WANT_MASTER:
            handle_want_master(data, size);
            break;
        case MID_C_ALIVE:
            handle_client_alive(data, size);
            break;
        default:
            error_and_fail("unknown message id");
    }
    zframe_destroy(&incoming);

    return 0;
}

int master_alive(zloop_t *loop, zmq_pollitem_t *item, void *arg) {
    struct timeval tim;
    int x;
    gettimeofday(&tim, NULL);
    zframe_t * frame;
    if (tim.tv_sec >= cstate.reset_timer)
        restart_election();
    
    if (cstate.role == ROLE_MASTER) {
        MSG_DOPACK(
        msgpack_pack_int(pk, MID_OBEY);
        MSG_PACK_STR(pk, g_hosts[0].host);

        frame = zframe_new(
            buf->data, buf->size);
        );

        for (x=0; x < num_hosts; x++) {
            zframe_t * sendframe = zframe_dup(frame);
            zframe_send(&sendframe, g_hosts[x].peer_socket, 0);
        }
        zframe_destroy(&frame);
    }

    return 0;
}

int election_runner(zloop_t *loop, zmq_pollitem_t *item, void *arg) {
    /* advocate for our candidate */
    if (cstate.role != ROLE_ELECTING)
        return 0;
    int x;
    zframe_t * frame;
    zclock_log("{election_runner} advocating for %s", cstate.advocating);

    MSG_DOPACK(
        msgpack_pack_int(pk, MID_ADVOCATE);
        MSG_PACK_STR(pk, myid);
        MSG_PACK_STR(pk, cstate.advocating);

        frame = zframe_new(buf->data, buf->size);
    );

    for (x=0; x < num_hosts; x++) {
        zframe_t * sendframe = zframe_dup(frame);
        zframe_send(&sendframe, g_hosts[x].peer_socket, 0);
    }

    zframe_destroy(&frame);

    return 0;
}

void loop() {
    mainloop = zloop_new();
    zmq_pollitem_t bind_input = {local_socket, 0, ZMQ_POLLIN};
    zloop_poller(mainloop, &bind_input, input_event, NULL);
    zloop_timer(mainloop, 1000, 0, election_runner, NULL);
    zloop_timer(mainloop, 1000, 0, master_alive, NULL);
    zloop_start(mainloop);
}
