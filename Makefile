CFLAGS=`mysql_config --cflags` `pkg-config --cflags glib-2.0` -Wall -O3 -g
LDFLAGS=`mysql_config --libs_r` `pkg-config --libs glib-2.0`

all: dump

clean:
	rm dump *~ *BAK

indent:
	gnuindent -ts4 -kr -l200 dump.c
