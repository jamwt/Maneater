#include <stdio.h>
#include <pthread.h>
#include <sys/time.h>
#include <assert.h>

#include "mt.h"

#define UUID_SIZE 36

static pthread_mutex_t mt_mutex;

void init_random_system() {
    long unsigned int randinit[2];
    struct timeval tim;
    gettimeofday(&tim, NULL);

    randinit[0] = tim.tv_sec;
    randinit[1] = tim.tv_usec;

    init_by_array(randinit, 2);

    pthread_mutex_init(&mt_mutex, NULL);
}

/* Implements RFC4122 UUID4s */
void generate_uuid4(char *out) {
    /* out must have at least 37 bytes allocated */
    assert(out); // at least check it's not NULL
    unsigned int buf[4];
    short unsigned int  * shorts = (short unsigned int *)buf;
    char *mod;

    pthread_mutex_lock(&mt_mutex);
    buf[0] = genrand_int32();
    buf[1] = genrand_int32();
    buf[2] = genrand_int32();
    buf[3] = genrand_int32();
    pthread_mutex_unlock(&mt_mutex);

    /* RFC standardizaitons */
    /* 1. mark version # */
    mod = (char*)buf + 7;
    *mod &= 0x0f;
    *mod |= 0x40;

    /* 2. Clock hi_and_res bit set */
    mod = (char*)buf + 9;
    *mod &= 0x3f; /* clear most sig 2 bits */
    *mod |= 1 << 7; /* set most sig to 1 (implies bit 6 is 0) */

    snprintf(out, UUID_SIZE + 1, "%04hx%04hx-%04hx-%04hx-%04hx-%04hx%04hx%04hx",
            shorts[0], shorts[1],
            shorts[2], shorts[3],
            shorts[4], shorts[5],
            shorts[6], shorts[7]);
    out[UUID_SIZE] = 0;
}
