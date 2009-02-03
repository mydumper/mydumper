CFLAGS=`mysql_config --cflags` `pkg-config --cflags glib-2.0 gthread-2.0` -Wall -O3 -g
LDFLAGS=`mysql_config --libs_r` `pkg-config --libs glib-2.0 gthread-2.0`

all: mydumper

mydumper: mydumper.o
	$(CC) -g -o mydumper mydumper.o $(LDFLAGS)

clean:
	rm -rf mydumper dump *~ *BAK *.dSYM *.o

indent:
	gnuindent -ts4 -kr -l200 dump.c
