#include "maneater.h"
#include <unistd.h>

int main(int argc, char **argv) {
    char *hosts[] = {"127.0.0.1:4441",
                "127.0.0.1:4442",
                "127.0.0.1:4443" };
    maneater_client * mc = maneater_client_new("127.0.0.1", hosts, 3);

    sleep(80);

    (void)mc;

    return 0;
}
