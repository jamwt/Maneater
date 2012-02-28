objects=`ls *.c | sed 's/\.c/\.o/g'`
redo-ifchange $objects
gcc -o $3 $objects -lczmq
