#include "maneater.h"
#include <unistd.h>

int main(int argc, char **argv) {
    char *hosts[] = {"127.0.0.1:4444"};
    maneater_client * mc = maneater_client_new("127.0.0.1", hosts, 1);

    sleep(10);

    (void)mc;

    return 0;
}
