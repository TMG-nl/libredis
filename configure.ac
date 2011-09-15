#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.63])
AC_INIT([libredis], [0.8], [henk@hyves.nl])
AM_INIT_AUTOMAKE([1.10 gnu])
AC_CONFIG_SRCDIR([libredis/config.h.in])
AC_CONFIG_HEADERS([libredis/config.h])
AC_CONFIG_MACRO_DIR([m4])

LT_INIT
AC_SUBST([LIBTOOL_DEPS])

LIBREDIS_RELEASE=0:0:8
LIBREDIS_LIBRARY_VERSION=1:0:0
#                        | | |
#                 +------+ | +---+
#                 |        |     |
#               current:revision:age
#                 |        |     |
#                 |        |     +- increment if interfaces have been added
#                 |        |        set to zero if interfaces have been removed
#                 |        |        or changed
#                 |        +- increment if source code has changed
#                 |           set to zero if current is incremented
#                 +- increment if interfaces have been added, removed or changed
AC_SUBST(LIBREDIS_RELEASE)
AC_SUBST(LIBREDIS_LIBRARY_VERSION)

# Checks for programs.
AC_PROG_LIBTOOL
AC_PROG_CC

# Checks for libraries.
AC_CHECK_LIB([rt], [clock_gettime],, [AC_MSG_ERROR([[librt not found]])])
AC_CHECK_LIB([m], [exp],, [AC_MSG_ERROR([[libm not found]])])

# Checks for header files.
AC_CHECK_HEADERS([arpa/inet.h, fcntl.h, netdb.h, netinet/in.h, stdlib.h, string.h, sys/socket.h, unistd.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_C_INLINE
AC_TYPE_SIZE_T
AC_TYPE_UINT64_T

# Checks for library functions.
AC_FUNC_ERROR_AT_LINE
AC_CHECK_FUNCS([clock_gettime memset socket strchr strerror])

AC_CONFIG_FILES([
        Makefile
        libredis/Makefile
        libredis/libredis.pc
])

AC_OUTPUT

