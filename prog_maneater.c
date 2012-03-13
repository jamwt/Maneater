#include "maneater.h"

int main (int argc, char **argv) {
    handle_args(argc, argv);
    setup_node_state();
    set_init();
    setup_zeromq();

    loop();

    return 0;
}
