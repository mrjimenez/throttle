
dnl Process this file with autoconf to produce a configure script.
AC_INIT(throttle, 1.2, james@klicman.org)
AM_INIT_AUTOMAKE(throttle, 1.2)
AM_CONFIG_HEADER(config.h)

dnl Checks for programs.
AC_PROG_CC
AC_PROG_INSTALL

dnl Checks for libraries.

dnl Checks for header files.
AC_HEADER_STDC
AC_CHECK_HEADERS([fcntl.h inttypes.h stdlib.h string.h sys/time.h unistd.h])

dnl Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STDBOOL
AC_TYPE_SIZE_T
AC_HEADER_TIME
AC_C_VOLATILE

dnl Checks for library functions.
AC_FUNC_MALLOC
AC_FUNC_REALLOC
AC_FUNC_STRTOD
AC_CHECK_FUNCS([clock_gettime gettimeofday memset mkfifo modf])
AC_SEARCH_LIBS(nanosleep, rt posix4, AC_DEFINE(HAVE_NANOSLEEP, 1,
    [Define to 1 if you have nanosleep]))

AC_CONFIG_FILES([Makefile])
AC_OUTPUT
