dnl $Id$
dnl config.m4 for extension libredis

dnl Comments in this file start with the string 'dnl'.
dnl Remove where necessary. This file will not work
dnl without editing.

dnl If your extension references something external, use with:

PHP_ARG_WITH(libredis, for libredis support,
[  --with-libredis             Include libredis support])

dnl Otherwise use enable:

dnl PHP_ARG_ENABLE(libredis, whether to enable libredis support,
dnl Make sure that the comment is aligned:
dnl [  --enable-libredis           Enable libredis support])

if test "$PHP_LIBREDIS" != "no"; then
	dnl # --with-libredis -> check with-path
	SEARCH_PATH="/usr/local /usr"     # you might want to change this
	SEARCH_FOR="/include/libredis/redis.h"  # you most likely want to change this
	
	if test -r $PHP_LIBREDIS/$SEARCH_FOR; then # path given as parameter
    	LIBREDIS_DIR=$PHP_LIBREDIS
		dnl # --with-libredis -> add include path
        PHP_ADD_INCLUDE($LIBREDIS_DIR/include)

		dnl # --with-libredis -> check for lib and symbol presence
		LIBNAME=redis # you may want to change this
		LIBSYMBOL=Module_new # you most likely want to change this

		PHP_CHECK_LIBRARY($LIBNAME,$LIBSYMBOL,
		[
			PHP_ADD_LIBRARY_WITH_PATH($LIBNAME, $LIBREDIS_DIR/lib, LIBREDIS_SHARED_LIBADD)
			AC_DEFINE(HAVE_LIBREDISLIB, 1, [whether libredis exists on the system])
		],[
			AC_MSG_ERROR([wrong libredis lib version or lib not found])
		],[
			-L$LIBREDIS_DIR/lib -lm -ldl -lrt
        ])
	else #we look using pkg-config which is much nicer 
		AC_MSG_CHECKING(for pkg-config)
		if test ! -f "$PKG_CONFIG"; then
			PKG_CONFIG=`which pkg-config`
		fi

		if test -f "$PKG_CONFIG"; then
			AC_MSG_RESULT(found)
			AC_MSG_CHECKING(for libredis)

			if $PKG_CONFIG --exists libredis; then
				libredis_version_full=`$PKG_CONFIG --modversion libredis`
				AC_MSG_RESULT([found $libredis_version_full])
				LIBREDIS_LIBS="$LDFLAGS `$PKG_CONFIG --libs libredis`"
				LIBREDIS_INCS="$CFLAGS `$PKG_CONFIG --cflags libredis`"
				LIBREDIS_PREFIX="`$PKG_CONFIG --variable=prefix libredis`"
				PHP_EVAL_INCLINE($LIBREDIS_INCS)
				PHP_EVAL_LIBLINE($LIBREDIS_LIBS, LIBREDIS_SHARED_LIBADD)
				AC_DEFINE(HAVE_LIBREDIS, 1, [whether libredis exists on the system])
			else
				AC_MSG_RESULT(not found)
				AC_MSG_ERROR(Ooops! no libredis detected!)
			fi
		else
			AC_MSG_RESULT(not found)
			AC_MSG_ERROR(Ooops! no pkg-config detected!)
		fi
	fi

	PHP_SUBST(LIBREDIS_SHARED_LIBADD)

	CFLAGS="-std=gnu99 $CFLAGS -pedantic -Wall -DNDEBUG -fvisibility=hidden"

	PHP_NEW_EXTENSION(libredis, libredis.c, $ext_shared)
fi
