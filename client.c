#include <stdlib.h>
#include <czmq.h>
#include <pthread.h>
#include <assert.h>
#include <sys/time.h>

#include "uuid.h"
#include "maneater.h"

#define MASTER_TIMEOUT 6

typedef enum {
    CS_FINDMASTER,
    CS_SYNC
} CLIENT_STATE;

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
        char *host = malloc(msg.data.via.raw.size + 1);
        strcpy(host, msg.data.via.raw.ptr);

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
        printf("sending to %p\n", cli->sockets[i]);
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

int steady_network_input(zloop_t *loop, zmq_pollitem_t *item, void *arg) {
    /* handle packets from network */

    return 0;
}

int master_pinger(zloop_t *loop, zmq_pollitem_t *item, void *arg) {
    /* handle packets from network */
    maneater_client *cli = (maneater_client *)arg;
    zframe_t *out;

    struct timeval tim;
    gettimeofday(&tim, NULL);
    if (tim.tv_sec - cli->last_tick > MASTER_TIMEOUT)
        return -1;

    printf("%s %s %p %p\n", cli->myhostid, cli->sessid,
            cli->master_socket, cli->sockets[0]);
    MSG_DOPACK(
        msgpack_pack_int(pk, MID_C_ALIVE);
        MSG_PACK_STR(pk, cli->myhostid);
        MSG_PACK_STR(pk, cli->sessid);

        out = zframe_new(buf->data, buf->size);
    );

    zframe_send(&out, cli->master_socket, 0);

    return 0;
}

CLIENT_STATE steady_state(maneater_client * cli) {
    zloop_t * mainloop = zloop_new();
    /* also inproc:// */
    zmq_pollitem_t bind_input = {cli->local_socket, 0, ZMQ_POLLIN};
    zloop_poller(mainloop, &bind_input, steady_network_input, (void *)cli);
    zloop_timer(mainloop, 1000, 0, master_pinger, (void *)cli);

    zloop_start(mainloop);

    // after master is lost
    zloop_destroy(&mainloop);

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
char *iface, char **hosts, int num_hosts) {
    if (!random_init) {
        init_random_system();
        random_init = 1;
    }
    int i;
    maneater_client * cli;

    cli = (maneater_client *)malloc(sizeof(maneater_client));
    cli->sessid = (char *)malloc(UUID_SIZE + 1);
    generate_uuid4(cli->sessid);
    cli->iface = iface;
    cli->ctx = zctx_new();
    cli->num_hosts = num_hosts;
    cli->hosts = (char **)malloc(sizeof(char *) * num_hosts);
    for (i=0; i < num_hosts; i++) {
        cli->hosts[i] = (char *)malloc(strlen(hosts[i]) + 1);
        strcpy(cli->hosts[i], hosts[i]);
    }

    pthread_create(&(cli->thread), NULL, client_run_loop, (void *)cli);

    return cli;
}
