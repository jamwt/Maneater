#ifndef MANEATER_H
#define MANEATER_H

typedef enum {
    ROLE_ELECTING,
    ROLE_SLAVE,
    ROLE_MASTER
} ROLE;

#define NODEID_LEN (8 + UUID_SIZE + 1)

typedef struct {
    char *host;
    void *peer_socket;
} consensus_host;

/* SETS */

void set_init();
void handle_set_message(void * msg, size_t len);

enum {
    MID_SET = 100,
    MID_VALUE
};


/* host | key | lock | limit or 0 | bytes */
#define TPL_SET_MAP "sssvB"

/* key | txid | [bytes]  */
#define TPL_VALUE_MAP "sUA(B)"

/* host | [key, txid] */
#define TPL_FOLLOW_MAP "sA(S(sU))"

#endif /* MANEATER_H */
