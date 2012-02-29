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
    char master[NODEID_LEN];
    char advocating[NODEID_LEN];
    unsigned int reset_timer;
    ROLE role;
    my_advocate *myads;
} consensus_state;

typedef struct {
    char msgtype;
    char nodeid[NODEID_LEN];
    char advocating[NODEID_LEN];
} message_advocate;

typedef struct {
    char msgtype;
    char nodeid[NODEID_LEN];
} message_obey;

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
        g_hosts[i].host = argv[i + 1];
    }
}

void restart_election() {
    zclock_log("{election_runner} (RE)START");
    cstate.reset_timer = 0xffffffff;
    cstate.master[0] = '\0';
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
    char connbuf[150] = {0};
    ctx = zctx_new();

    /* setup local bind */
    consensus_host *h = &(g_hosts[0]); // convention: I am the first
    local_socket = zsocket_new(ctx, ZMQ_DEALER);
    snprintf(connbuf, sizeof(connbuf), "tcp://%s", h->host);

    /* Note: no checking for error here b/c czmq will assert on invalid bind */
    conn_succ = zsocket_bind(local_socket, connbuf);

    /* setup peer connections (including to myself) */
    for (i=0; i < num_hosts; i++) {
        h = g_hosts + i;
        h->peer_socket = zsocket_new(ctx, ZMQ_DEALER);
        snprintf(connbuf, sizeof(connbuf), "tcp://%s", h->host);
        conn_succ = zsocket_connect(h->peer_socket, connbuf);
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
    }
}

void handle_advocate(message_advocate *ad) {

    // we haven't given up on the master yet?
    if (cstate.role != ROLE_ELECTING)
        return;

    zclock_log("got advocation: %s vs. %s", ad->advocating, cstate.advocating);

    /* 1. If this is LOWER than my current advocating value
     * I advocate for it now */
    if (strcmp(ad->advocating, cstate.advocating) < 0) {
        zclock_log("changing advocation: %s -> %s", cstate.advocating, ad->advocating);
        strcpy(cstate.advocating, ad->advocating);
        struct timeval tim;
        gettimeofday(&tim, NULL);
        cstate.reset_timer = tim.tv_sec + ELECTION_RESET_TIMEOUT;
    }

    /* 2. If this node is me, then add this to my set
     * of advocates; if I get a quorum, declare myself
     * master */
    
    my_advocate * fad;

    if (! strcmp(ad->advocating, myid) ) {
        HASH_FIND_STR(cstate.myads, ad->nodeid, fad);
        if (! fad) {
            zclock_log("new advocate for me: %s", ad->nodeid);
            fad = malloc(sizeof(my_advocate));
            strcpy(fad->nodeid, ad->nodeid);
            HASH_ADD_STR(cstate.myads, nodeid, fad);
            test_for_master();
        }
    }
}

void handle_obey(message_obey * ob) {
    struct timeval tim;
    gettimeofday(&tim, NULL);
    cstate.reset_timer = tim.tv_sec + ELECTION_RESET_TIMEOUT;

    if (cstate.role == ROLE_ELECTING) {
        cstate.role = ROLE_SLAVE;
        strcpy(cstate.master, ob->nodeid);
        zclock_log("SLAVE (master = %s)", cstate.master);
    }

}

int input_event(zloop_t *loop, zmq_pollitem_t *item, void *arg) {

    zframe_t *incoming = zframe_recv(local_socket);
    unsigned char *data = zframe_data(incoming);
    unsigned char msgid = data[0];

    switch (msgid) {
        case MID_ADVOCATE:
            handle_advocate((message_advocate *)data);
            break;
        case MID_OBEY:
            handle_obey((message_obey *)data);
            break;
        case MID_SET:
            handle_set_message(data + 1, zframe_size(incoming) - 1);
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
    if (tim.tv_sec >= cstate.reset_timer)
        restart_election();
    
    if (cstate.role == ROLE_MASTER) {
        message_obey m;
        m.msgtype = MID_OBEY;
        strcpy(m.nodeid, myid);
        zframe_t * frame = zframe_new((char *)&m, sizeof(m));

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
    zclock_log("{election_runner} advocating for %s", cstate.advocating);
    message_advocate m;
    m.msgtype = MID_ADVOCATE;
    strcpy(m.nodeid, myid);
    strcpy(m.advocating, cstate.advocating);
    zframe_t * frame = zframe_new((char *)&m, sizeof(m));
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

int main (int argc, char **argv) {
    handle_args(argc, argv);
    setup_node_state();
    set_init();
    setup_zeromq();

    loop();

    return 0;
}
