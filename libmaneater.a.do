OURLIBS=`ls *.c | egrep -v "prog_*" | sed 's/.c$/.o/'`
redo-ifchange CC $OURLIBS
ar rcs $3 $OURLIBS
