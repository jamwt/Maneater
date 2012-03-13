redo-ifchange libmaneater.a
redo-ifchange prog_test.o
gcc -o $3 -L. prog_test.o -lmaneater -lczmq -lpthread -lmsgpack
