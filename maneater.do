redo-ifchange CC libmaneater.a
redo-ifchange prog_maneater.o
gcc -o $3 -L. prog_maneater.o -lmaneater -lczmq -lpthread -lmsgpack
