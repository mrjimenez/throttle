/*
 * throttle: bandwidth limiting pipe
 * Copyright (C) 2003 - 2005  James Klicman <james at klicman dot org>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

#if HAVE_CONFIG_H
# include "config.h"
#endif


#if STDC_HEADERS
# include <stdio.h>
# include <errno.h>
# include <signal.h>
# include <math.h>
#endif
#if HAVE_STDLIB_H
# include <stdlib.h>
#endif
#if HAVE_STRING_H
# include <string.h>
#endif
#if HAVE_FCNTL_H
# include <fcntl.h>
#endif
#if HAVE_INTTYPES_H
# include <inttypes.h>
#endif
#if HAVE_SYS_TYPES_H
# include <sys/types.h>
#endif
#if HAVE_SYS_STAT_H
# include <sys/stat.h>
#endif
#if TIME_WITH_SYS_TIME
# include <sys/time.h>
# include <time.h>
#else
# if HAVE_SYS_TIME_H
#  include <sys/time.h>
# else
#  include <time.h>
# endif
#endif
#if HAVE_UNISTD_H
# include <unistd.h>
#endif


#define THROTTLE PACKAGE


#define DEFBLOCKSIZE 512


#define MESSAGE_MAGIC  'T'
/* The high-byte and low-byte of MESSAGE_VERSION should always differ. This
 * will prevent messages from being decoded which are received via FIFO over
 * NFS from a client on a different endian machine. The messages are in a
 * binary format and are not designed for FIFOs on NFS mounted filesystems.
 */
#define MESSAGE_VERSION 0x0102



/* lightweight C exception handling hack */
#define THROW(func, exception) goto func##_catch_##exception
#define THROW(func, exception) goto func##_catch_##exception
#define CATCH(func, exception) func##_catch_##exception
#define END(func) goto func##_finally
#define FINALLY(func) func##_finally


/* time abstraction macros */
#if (HAVE_CLOCK_GETTIME && HAVE_NANOSLEEP)
#define TIME_STRUCT timespec
#define TIME_SUBSEC(tv) (tv).tv_nsec
#define TIME_1SEC_IN_SUBSECS 1000000000
#define TIME_GET(tv) clock_gettime(CLOCK_REALTIME,(tv))
#define TIME_SLEEP(tv) nanosleep((tv),(tv))
#else
#define TIME_STRUCT timeval
#define TIME_SUBSEC(tv) (tv).tv_usec
#define TIME_1SEC_IN_SUBSECS 1000000
#define TIME_GET(tv) gettimeofday((tv), NULL)
#define TIME_SLEEP(tv) select(0,NULL,NULL,NULL,(tv))
#endif

/* BSECS: how many seconds does it take to send bytes at Bps */
#define BSECS(bytes,Bps) (((double)(bytes))/((double)(Bps)))

/* tsecs: convert time struct to double in seconds */
#define TSECS(tvp) (((double)(tvp).tv_sec)+(((double)TIME_SUBSEC(tvp))/((double)(TIME_1SEC_IN_SUBSECS))))

#define TIMERADD(a, b, result)                             \
    (result).tv_sec = (a).tv_sec + (b).tv_sec;             \
    TIME_SUBSEC(result) = TIME_SUBSEC(a) + TIME_SUBSEC(b); \
    if (TIME_SUBSEC(result) >= TIME_1SEC_IN_SUBSECS) {     \
	(result).tv_sec++;                                 \
	TIME_SUBSEC(result) -= TIME_1SEC_IN_SUBSECS;       \
    }
#define TIMERSUB(a, b, result)                             \
    (result).tv_sec = (a).tv_sec - (b).tv_sec;             \
    TIME_SUBSEC(result) = TIME_SUBSEC(a) - TIME_SUBSEC(b); \
    if (TIME_SUBSEC(result) < 0) {                         \
	(result).tv_sec--;                                 \
	TIME_SUBSEC(result) += TIME_1SEC_IN_SUBSECS;       \
    }


typedef enum { false = 0, true = 1 } boolean;


typedef enum {
    CHANGE_NONE		= 0,
    CHANGE_UNIT		= 1,
    CHANGE_LIMIT	= 1<<1,
    CHANGE_BLOCKSIZE	= 1<<2,
    CHANGE_WINDOW	= 1<<3,
    CHANGE_VERBOSE	= 1<<4
} ChangeArgs;


struct fifomsg {
    char magic;
    char type;
    unsigned short version;
    union {
	double Bps;
	double unit;
	time_t window;
	size_t blocksize;
	boolean verbose;
    } data;
};


static boolean verbose = false; /* print verbose messages to stderr */
static double unit;
static char unit_opt;
static char* fifoname;

volatile boolean showstats = false; /* show stats on next iteration, don't wait for window */
volatile boolean checkfifo = false; /* check fifo on next iteration, don't wait for window */


/*
 * signal handler to clean up fifo
 */
static void sig_exit(int sig)
{
    if (fifoname != NULL)
	unlink(fifoname);

    _exit(sig);
}

/*
 * signal handler causes stats to display in next iteration without waiting for
 * the beginning of the next window.
 */
static void sig_usr1(int sig)
{
    showstats = true;
}

/*
 * signal handler which causes the fifo to be checked for messages without
 * waiting for the beginning of the next window.
 */
static void sig_usr2(int sig)
{
    checkfifo = true;
}

static int writemsg(int fd, struct fifomsg *msg)
{
    int nwrite;

    msg->magic   = MESSAGE_MAGIC;
    msg->version = MESSAGE_VERSION;

    if ((nwrite = write(fd, msg, sizeof(struct fifomsg))) == -1) {
	perror(THROTTLE ": write fifo");
	return 1;
    }

    if (nwrite != sizeof(struct fifomsg)) {
	fprintf(stderr, THROTTLE ": write fifo failed: %d bytes written", nwrite);
	return 1;
    }

    return 0;
}


static int fifosend(ChangeArgs changeargs, double Bps, time_t window, size_t blocksize)
{
    int fd;
    struct fifomsg msg;
    int retval;


    if ((fd = open(fifoname, O_WRONLY)) < 0) {
	perror(THROTTLE ": open fifo");
	return -1;
    }

    retval = -1; /* init to -1 in case of exception */
    /*
     * from this point on use THROW instead of return
     */

    /* send verbose first to enable/disable messages for other changes */
    if (changeargs & CHANGE_VERBOSE) {
	memset(&msg, 0, sizeof(msg));
	msg.type = 'v';
	msg.data.verbose = verbose;
	if (writemsg(fd, &msg) != 0)
	    THROW(fifosend, write_error);
    }

    if (changeargs & CHANGE_UNIT) {
	memset(&msg, 0, sizeof(msg));
	msg.type = unit_opt;
	msg.data.unit = unit;
	if (writemsg(fd, &msg) != 0)
	    THROW(fifosend, write_error);
    }

    if (changeargs & CHANGE_LIMIT) {
	memset(&msg, 0, sizeof(msg));
	msg.type = '<';
	msg.data.Bps = Bps;
	if (writemsg(fd, &msg) != 0)
	    THROW(fifosend, write_error);
    }

    if (changeargs & CHANGE_WINDOW) {
	memset(&msg, 0, sizeof(msg));
	msg.type = 'w';
	msg.data.window = window;
	if (writemsg(fd, &msg) != 0)
	    THROW(fifosend, write_error);
    }

    if (changeargs & CHANGE_BLOCKSIZE) {
	memset(&msg, 0, sizeof(msg));
	msg.type = 's';
	msg.data.blocksize = blocksize;
	if (writemsg(fd, &msg) != 0)
	    THROW(fifosend, write_error);
    }

    retval = 0;

CATCH(fifosend, write_error):

    close(fd);

    return retval;
}


static int fiforecv(int fd, double *Bps, time_t *window, size_t *blocksize)
{
    int nread;
    struct fifomsg msg;
    boolean msgok;

    do {
	if ((nread = read(fd, &msg, sizeof(struct fifomsg))) <= 0)
	    return nread;

	msgok = false;
	if (nread == sizeof(struct fifomsg) &&
	    msg.magic == MESSAGE_MAGIC &&
	    msg.version == MESSAGE_VERSION)
	{
	    msgok = true;
	    switch (msg.type) {
	    case 'b':
	    case 'k':
	    case 'm':
	    case 'B':
	    case 'K':
	    case 'M':
		if (msg.type != unit_opt) {
		    if (verbose)
			fprintf(stderr, THROTTLE ": unit changed from %c/s to %c/s\n",
				unit_opt, msg.type);
		    unit = msg.data.unit;
		    unit_opt = msg.type;
		}
		break;
	    case '<':
		if (msg.data.Bps != *Bps) {
		    if (verbose)
			fprintf(stderr, THROTTLE ": limit changed from %f %c/s to %f %c/s\n",
				(*Bps) / unit, unit_opt, msg.data.Bps / unit, unit_opt);
		    *Bps = msg.data.Bps;
		}
		break;
	    case 'w':
		if (msg.data.window != *window) {
		    if (verbose)
			fprintf(stderr, THROTTLE ": window changed from %ld to %ld\n",
				(long)*window, (long)msg.data.window);
		    *window = msg.data.window;
		}
		break;
	    case 's':
		if (msg.data.blocksize != *blocksize) {
		    if (verbose)
			fprintf(stderr, THROTTLE ": blocksize changed from %lu to %lu\n",
				(unsigned long)*blocksize, (unsigned long)msg.data.blocksize);
		    *blocksize = msg.data.blocksize;
		}
		break;
	    case 'v':
		if (msg.data.verbose != verbose) {
		    if (msg.data.verbose)
			fprintf(stderr, THROTTLE ": verbose changed from %d to %d\n",
				verbose, msg.data.verbose);
		    verbose = msg.data.verbose;
		}
		break;
	    default:
		msgok = false;
		break;
	    }
	}

	if (nread > 0 && !msgok) {
	    fprintf(stderr, THROTTLE ": invalid fifo message received\n");
	}

    } while (nread > 0);

    return 0;
}


static uint64_t unitdiv(uint64_t n, char *nunit)
{
    static char units[] = "BKMGTPE";
    uint64_t unitmax;
    uint64_t unitdiv;
    int i;

    unitmax = 10000;
    unitdiv = 1;
    for (i = 0; i < sizeof(units)-2; i++) {
	if (n < unitmax)
	    break;
	unitmax *= 1024;
	unitdiv *= 1024;
    }
    *nunit = units[i];
    return (n / unitdiv);
}


static int printstats(uint64_t totalbytes, uint64_t byteswritten, struct TIME_STRUCT elapsedtime,
    size_t blocksize, time_t window, double Bps)
{
    double tsecs;
    uint64_t total, written;
    char total_unit, written_unit;

    total = unitdiv(totalbytes, &total_unit);
    written = unitdiv(byteswritten, &written_unit);

    tsecs = TSECS(elapsedtime);

    return fprintf(stderr, THROTTLE ": %4llu%c, %4llu%c in %.3fs %.3f%c/s -s %lu -w %ld -%c %.3f\n",
	    total, total_unit, written, written_unit, tsecs, BSECS(byteswritten, tsecs) / unit, unit_opt,
	    (unsigned long)blocksize, (long)window, unit_opt, Bps / unit);
}


static int throttle(double Bps, time_t window, size_t blocksize, int fd)
{
    void *block, *newblock;
    size_t newblocksize;
    int32_t bytesread;
    uint64_t byteswritten, totalbytes;
    size_t nleft;
    ssize_t nread, nwritten;
    char *ptr;
    boolean done;
    double sync, syncmin, integral, fractional;
    struct TIME_STRUCT starttime, currenttime, elapsedtime, synctime;
    int retval;

    /* estimate sync resolution */
    synctime.tv_sec = 0;
    TIME_SUBSEC(synctime) = 1;
    retval  = TIME_GET(&starttime);
    retval |= TIME_SLEEP(&synctime);
    retval |= TIME_GET(&currenttime);
    retval |= TIME_SLEEP(&synctime);
    retval |= TIME_GET(&elapsedtime);
    if (retval != 0) {
	perror(THROTTLE ": estimate sync min");
	return -1;
    }
    /* calculate average resolution */
    TIMERSUB(currenttime,starttime,starttime);
    TIMERSUB(elapsedtime,currenttime,currenttime);
    TIMERADD(starttime,currenttime,currenttime);
    /* syncavg = (time1 + time2) / 2
     * syncmin = syncavg / 3
     */
    syncmin = (TSECS(currenttime) / 2) / 3;
    if (verbose)
	fprintf(stderr, THROTTLE ": sync min = %.9f\n", syncmin);

    /* init start time to results of last TIME_GET */
    starttime = elapsedtime;

    if ((block = malloc(blocksize)) == NULL) {
	perror(THROTTLE ": malloc");
	return -1;
    }

    retval = -1; /* init to -1 in case of exception */
    /*
     * from this point on use THROW instead of return
     */

    elapsedtime.tv_sec = window; /* start main loop off on the right foot */
    bytesread = 0;
    totalbytes = byteswritten = 0;
    newblocksize = blocksize;

    done = false;
    do { /* while (!done) */
	if (blocksize != newblocksize) {
	    if ((newblock = realloc(block, newblocksize)) == NULL) {
		perror(THROTTLE ": remalloc failed to change blocksize");
		newblocksize = blocksize; /* keep old blocksize */
	    } else {
		block = newblock;
		blocksize = newblocksize;
	    }
	}

	ptr = block;
	nleft = blocksize;
	do {
	    nread = read(STDIN_FILENO, ptr, nleft);
	    if (nread < 0) {
		if (errno == EINTR)
		    continue;
		THROW(throttle, read_error);
	    }

	    if (nread == 0) {
		done = true;
		break; /* EOF */
	    }

	    nleft -= nread;
	    ptr += nread;
	} while (nleft > 0);

	nread = (blocksize - nleft);
	bytesread += nread;

	if (elapsedtime.tv_sec >= window ||
	    bytesread < 0 /* check rollover */)
	{
	    if (TIME_GET(&currenttime) == -1)
		THROW(throttle, gettime_error);

	    if (verbose || showstats) {
		TIMERSUB(currenttime, starttime, elapsedtime);
		printstats(totalbytes, byteswritten, elapsedtime, blocksize, window, Bps);
		showstats = false;
	    }

	    if (fd >= 0)
		fiforecv(fd, &Bps, &window, &newblocksize);

	    starttime = currenttime;
	    elapsedtime.tv_sec = 0;
	    TIME_SUBSEC(elapsedtime) = 0;
	    bytesread = nread;
	    byteswritten = 0;

	    sync = BSECS(bytesread, Bps);
	}
	else
	{
resync:
	    if (fd >= 0 && checkfifo) {
		fiforecv(fd, &Bps, &window, &newblocksize);
		checkfifo = false;
	    }

	    if (TIME_GET(&currenttime) == -1)
		THROW(throttle, gettime_error);

	    TIMERSUB(currenttime, starttime, elapsedtime);

	    if (showstats) {
		printstats(totalbytes, byteswritten, elapsedtime, blocksize, window, Bps);
		showstats = false;
	    }

	    sync = BSECS(bytesread, Bps) - TSECS(elapsedtime);
	}
	if (sync >= syncmin) {
	    fractional = modf(sync, &integral);
	    synctime.tv_sec = (time_t)integral;
	    TIME_SUBSEC(synctime) = (long)(fractional * TIME_1SEC_IN_SUBSECS);
	    if (TIME_SUBSEC(synctime) >= TIME_1SEC_IN_SUBSECS)
		TIME_SUBSEC(synctime) = (TIME_1SEC_IN_SUBSECS - 1);

	    if (TIME_SLEEP(&synctime) < 0) {
		if (errno == EINTR) {
		    goto resync;
		} else {
		    THROW(throttle, sleep_error);
		}
	    }
	}

	ptr = block;
	nleft = nread;
	while (nleft > 0) {
	    nwritten = write(STDOUT_FILENO, ptr, nleft);
	    if (nwritten <= 0) {
		if (errno == EINTR)
		    continue;
		THROW(throttle, write_error);
	    }

	    nleft -= nwritten;
	    ptr += nwritten;
	}
	byteswritten += (nread - nleft);

	totalbytes += (nread - nleft);

    } while (!done);

    retval = 0; /* success */

    END(throttle);

CATCH(throttle, gettime_error):
    perror(THROTTLE ": gettime");
END(throttle);

CATCH(throttle, read_error):
    perror(THROTTLE ": read");
END(throttle);

CATCH(throttle, write_error):
    perror(THROTTLE ": write");
END(throttle);

CATCH(throttle, sleep_error):
    perror(THROTTLE ": sync sleep");
END(throttle);

FINALLY(throttle):
    free(block);

    return retval;
}


static void usage() {
    fprintf(stderr,
	"Usage: " THROTTLE " [-l fifo | -t fifo] [-s blocksize] [-w window] [-bkmBKM] limit\n"
	"     limit      - Bandwidth limit.\n"
	"  -b, -k, -m    - bits, kilobits or megabits per second.\n"
	"  -B, -K, -M    - Bytes, Kilobytes or Megabytes per second.\n"
	"  -s blocksize  - Block size for input and output.\n"
	"  -w window     - Window of time in seconds.\n"
	"  -l fifo       - Create fifo and listen for change option messages.\n"
	"  -t fifo       - Transmit options to existing throttle process.\n"
	"  -q            - Run quietly.\n"
	"  -v            - Print informational messages to stderr at window intervals.\n"
	"  -V            - Print the version number and copyright and exit.\n"
	"  -h            - Display this message and exit.\n");
}


int main(int argc, char **argv)
{
    int c, err;
    double limit, Bps;
    long larg;
    time_t window;
    size_t blocksize;
    enum { SERVER = 0, CLIENT = 1 } fifotype;
    ChangeArgs changeargs;
    int fd;
    struct sigaction sigact;

    Bps = 0.0; /* init to zero */

    /* defaults */
    unit = 1.0; /* bytes per second */
    unit_opt = 'B';
    window = 60;
    blocksize = DEFBLOCKSIZE;
    fifoname = NULL;
    fifotype = SERVER;
    changeargs = CHANGE_NONE;
    fd = -1;

    while ((c = getopt(argc, argv, "s:w:l:t:bkmBKMqvVh")) != -1) {
	switch (c) {

	case 's':
	    if ((larg = atol(optarg)) < 1) {
		fprintf(stderr, THROTTLE ": invalid blocksize %s\n", optarg);
		return EXIT_FAILURE;
	    }
	    blocksize = (size_t)larg;
	    changeargs |= CHANGE_BLOCKSIZE;
	    break;

	case 'w':
	    if ((larg = atol(optarg)) < 1) {
		fprintf(stderr, THROTTLE ": invalid window size %s\n", optarg);
		return EXIT_FAILURE;
	    }
	    window = (time_t)larg;
	    changeargs |= CHANGE_WINDOW;
	    break;

	case 'l':
	    fifoname = optarg;
	    fifotype = SERVER;
	    break;

	case 't':
	    fifoname = optarg;
	    fifotype = CLIENT;
	    break;

	case 'b':
	    unit = 1.0/8.0;
	    unit_opt = c;
	    changeargs |= CHANGE_UNIT;
	    break;

	case 'k':
	    unit = 1024.0/8.0;
	    unit_opt = c;
	    changeargs |= CHANGE_UNIT;
	    break;

	case 'm':
	    unit = (1024.0*1024.0)/8.0;
	    unit_opt = c;
	    changeargs |= CHANGE_UNIT;
	    break;

	case 'B':
	    unit = 1.0;
	    unit_opt = c;
	    changeargs |= CHANGE_UNIT;
	    break;

	case 'K':
	    unit = 1024.0;
	    unit_opt = c;
	    changeargs |= CHANGE_UNIT;
	    break;

	case 'M':
	    unit = 1024.0*1024.0;
	    unit_opt = c;
	    changeargs |= CHANGE_UNIT;
	    break;

	case 'q':
	    verbose = false;
	    changeargs |= CHANGE_VERBOSE;
	    break;

	case 'v':
	    verbose = true;
	    changeargs |= CHANGE_VERBOSE;
	    break;

	case 'V':
	    fprintf(stdout,
		THROTTLE " " VERSION "\n"
		"Copyright 2003 - 2005 James Klicman <james@klicman.org>\n"
		"This is free software; see the source for copying conditions.  There is NO\n"
		"warranty; not even for MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.\n");
	    return 0;

	case 'h':
	default:
	    usage();
	    return EXIT_FAILURE;
	}
    }

    if (optind == (argc - 1)) {
	limit = strtod(argv[optind], (char **)NULL);
	if (limit <= 0.0) {
	    fprintf(stderr, THROTTLE ": invalid limit %s\n", argv[optind]);
	    return EXIT_FAILURE;
	}

	Bps = limit * unit;
	changeargs |= CHANGE_LIMIT;
    } else if (fifotype == CLIENT) {
	/* client not required to change limit */
    } else {
	usage();
	return EXIT_FAILURE;
    }


    if (fifotype == CLIENT) {
	if (changeargs == CHANGE_NONE) {
	    fprintf(stderr, THROTTLE ": no changes\n");
	    return EXIT_FAILURE;
	}

	err = fifosend(changeargs, Bps, window, blocksize);
    } else {
	if (fifoname != NULL) {
	    sigact.sa_handler = sig_exit;
	    sigfillset(&sigact.sa_mask); /* block all other signals duing sig_exit */
	    sigact.sa_flags = 0;

	    if (sigaction(SIGHUP, &sigact, (struct sigaction*)NULL) ||
		sigaction(SIGINT, &sigact, (struct sigaction*)NULL) ||
		sigaction(SIGPIPE, &sigact, (struct sigaction*)NULL) ||
		sigaction(SIGTERM, &sigact, (struct sigaction*)NULL) ||
		sigaction(SIGQUIT, &sigact, (struct sigaction*)NULL))
	    {
		perror(THROTTLE ": sigaction");
		return EXIT_FAILURE;
	    }

	    sigact.sa_handler = sig_usr1;
	    sigemptyset(&sigact.sa_mask);
	    sigaddset(&sigact.sa_mask, SIGUSR1);
	    sigaddset(&sigact.sa_mask, SIGUSR2);
	    sigact.sa_flags = 0;
	    if (sigaction(SIGUSR1, &sigact, (struct sigaction*)NULL)) {
		perror(THROTTLE ": sigaction");
		return EXIT_FAILURE;
	    }

	    sigact.sa_handler = sig_usr2;
	    if (sigaction(SIGUSR2, &sigact, (struct sigaction*)NULL)) {
		perror(THROTTLE ": sigaction");
		return EXIT_FAILURE;
	    }

	    if (mkfifo(fifoname, 0666) != 0) {
		perror(THROTTLE ": mkfifo");
		return EXIT_FAILURE;
	    }

	    if ((fd = open(fifoname, O_RDWR|O_NONBLOCK)) < 0) {
		perror(THROTTLE ": open fifo");
		unlink(fifoname);
		return EXIT_FAILURE;
	    }
	}

	err = throttle(Bps, window, blocksize, fd);

	if (fifoname != NULL) {
	    close(fd);
	    if (unlink(fifoname) < 0)
		perror(THROTTLE ": unlink fifo");
	}
    }

    return (err == 0 ? EXIT_SUCCESS : EXIT_FAILURE);
}

/* vim:set ts=8 sw=4 softtabstop=4: */
