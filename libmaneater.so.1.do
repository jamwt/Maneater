OURLIBS=`ls *.c | egrep -v "prog_*" | sed 's/.c$/.o/'`
redo-ifchange CC $OURLIBS
gcc -shared -Wl,-soname,libmaneater.so.1 -o $3 $OURLIBS -lczmq -lpthread -lmsgpack
