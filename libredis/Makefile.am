ACLOCAL_AMFLAGS=-I m4

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = libredis.pc

nobase_include_HEADERS = redis.h
noinst_HEADERS = alloc.h batch.h buffer.h common.h connection.h ketama.h list.h \
                 module.h parser.h reply.h

lib_LTLIBRARIES = libredis.la
libredis_la_SOURCES = redis.h alloc.h batch.c batch.h buffer.c \
                      buffer.h common.h connection.c connection.h \
                      ketama.c ketama.h list.h md5.c md5.h module.c \
                      module.h parser.c parser.h reply.h

libredis_la_CFLAGS = ${AM_CFLAGS} $(LIBS) -std=gnu99
libredis_la_LDFLAGS = ${AM_LDFLAGS} -version-info \
                               $(LIBREDIS_LIBRARY_VERSION) 
