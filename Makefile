# Build flags
CFLAGS=-Wall -Werror `mysql_config --cflags` `pkg-config --cflags glib-2.0 gthread-2.0` `pcre-config --cflags`
LDFLAGS=`mysql_config --libs_r` `pkg-config --libs glib-2.0 gthread-2.0` `pcre-config --libs`
OPTFLAGS=-O3 -g

# Various defines
NAME=mydumper
VERSION = $(shell grep 'define VERSION' mydumper.c | cut -d'"' -f2)
CLEANFILES= $(NAME) dump *~ *BAK *.dSYM *.o
DISTFILES = Makefile $(NAME).c README
bindir = $(prefix)/bin
distdir = $(NAME)-$(VERSION)
prefix = /usr/local

# Required programs
CP = /bin/cp
GZIP = /bin/gzip
INSTALL = /usr/bin/install
INSTALL_PROGRAM = $(INSTALL) -m 755
RM = /bin/rm
TAR = /bin/tar

all: $(NAME)

mydumper: mydumper.o
	$(CC) $(CFLAGS) $(OPTFLAGS) -o $(NAME) $(NAME).o $(LDFLAGS)

install: all
	test -d $(DESTDIR)$(bindir) || $(INSTALL) -d $(DESTDIR)$(bindir)
	$(INSTALL_PROGRAM) $(NAME) $(DESTDIR)$(bindir)

distdir:
	if test -d $(distdir) ; then $(RM) -rf $(distdir) ; fi
	mkdir $(distdir)
	$(CP) -a $(DISTFILES) $(distdir)

dist: distdir
	$(TAR) chof - $(distdir) | $(GZIP) -c > $(distdir).tar.gz
	$(RM) -rf $(distdir)

uninstall:
	$(RM) -f $(DESTDIR)$(bindir)/$(NAME)

clean:
	$(RM) -f $(CLEANFILES)

maintainer-clean: clean
	$(RM) -f $(distdir).tar.gz
