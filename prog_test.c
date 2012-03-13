#include "maneater.h"
#include <unistd.h>

void value_changed(maneater_client *cli,
    const char *key,
    maneater_bin *results, int num_results) {
    int i;
    printf("%s changed:\n", key);

    for (i=0; i < num_results; i++) {
        printf("  %d: %s\n", i, (char*)results[i].ptr);
    }
}

int main(int argc, char **argv) {
    char *hosts[] = {"127.0.0.1:4441",
                "127.0.0.1:4442",
                "127.0.0.1:4443" };
    maneater_client * mc = maneater_client_new(
            "127.0.0.1", hosts, 3, &value_changed);

    sleep(3);

    maneater_client_sub(mc, "foo");
    
    sleep(2);
    maneater_client_set(mc, "foo", "bar", 4, 0, 0);

    sleep(80);

    (void)mc;

    return 0;
}
