redo-ifchange tpl.c tpl.h
gcc -O2 -g -fno-strict-aliasing -fPIC -Wall -Werror -o "$3" -c tpl.c
