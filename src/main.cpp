#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <sys/types.h>
#include <getopt.h>
#include <errno.h>
#include <limits.h>
#include <unistd.h>
#include <inttypes.h>
#include <openssl/md5.h>
#include <sys/time.h>
#include <sys/resource.h>

#include <map>
#include <string>

#include "btree.hpp"
#include "misc.hpp"
#include "storage.hpp"
#include "token_reader.hpp"
 
#include <ctype.h>
#include "lmdb.h"
#ifdef _WIN32
#define Z	"I"
#else
#define Z	"z"
#endif
static char *prog;


struct options {
	size_t blockSize;
	char forceCreateNewDb;
	char urlMode;
	char verbose;
	char invert;
	size_t cacheSize;
	size_t prefetchSize;
	unsigned char keySize;

	char checkMode;
	char format_seen;
	size_t map_size;

	// fields control
	int keyField;
	int keyFieldSeparator;

	TokenReader *reader;
};

struct statistic {
	size_t lineNumber;
};
/* ------------------------------------- */

static struct options OPTS;
static struct statistic STAT;

void usage();
const char *getHost(const char *url, size_t len);
const unsigned char *getHash(const char *string, int string_len);
int getStdinLine(char *buf, int buf_size, char **line_start, int *line_len);
void onSignal(int sig);
void onAlarm(int sig);
size_t parseSize(const char *str);
void mainLoop(MDB_env *env);

int main(int argc, char *argv[]) {
	const char *filename = "";
	unsigned long blockSize;
	unsigned long keySize;
	unsigned long keyField;
	size_t cacheSize;
	unsigned long prefetchSize;

	char ch;
	
	prog = argv[0];

	STAT.lineNumber = 0;

	OPTS.blockSize = 1024*8;
	OPTS.forceCreateNewDb = 0;
	OPTS.verbose = 0;
	OPTS.invert = 0;
	OPTS.urlMode = 0;
	OPTS.keySize = 8;
	OPTS.checkMode = 0;
	OPTS.format_seen = 0;
	OPTS.cacheSize = SIZE_T_MAX;
	OPTS.map_size = (size_t)10485760 * 100 * 16; // 16Gb

	// fields control
	OPTS.keyField   = 0;
	OPTS.keyFieldSeparator = '\t';

	while ((ch = getopt(argc, argv, "crVub:k:t:f:d:m:p:vsl:")) != -1) {
		switch (ch) {
			case 'b':
				blockSize = strtoul(optarg, NULL, 0);
				if(blockSize < 32 || blockSize == ULONG_MAX)
					fatalInUserOptions("Block size must be >32\n");

				OPTS.blockSize = blockSize;
			break;
			case 'k':
				keySize = strtoul(optarg, NULL, 0);
				if(keySize != 1 && keySize != 2 && keySize != 4 && keySize != 8 && keySize != 16)
					fatalInUserOptions("Key size must be in (1, 2, 4, 8, 16)\n");

				OPTS.keySize = keySize;
			break;
			case 'l':
				OPTS.map_size = strtoul(optarg, NULL, 0);
			break;
			case 't':
				filename = optarg;
			break;
			case 'c':
				OPTS.forceCreateNewDb = 1;
			break;
			case 'r':
				OPTS.checkMode = 1;
			break;
			case 'u':
				OPTS.urlMode = 1;
			break;
			case 's':
				OPTS.format_seen = 1;
			break;
			case 'V':
				OPTS.verbose = 1;
			break;
			case 'v':
				OPTS.invert = 1;
			break;
			case 'f':
				keyField = strtoul(optarg, NULL, 0);

				if(keyField == ULONG_MAX || keyField > INT_MAX)
					fatalInUserOptions("Field must be int");

				OPTS.keyField = keyField;
			break;
			case 'm':
				cacheSize = parseSize(optarg);

				if(cacheSize == SIZE_T_MAX)
					fatalInUserOptions("Cache size must be positive");

				OPTS.cacheSize = cacheSize;
			break;
			case 'p':
				prefetchSize = strtoul(optarg, NULL, 0);

				if(prefetchSize == ULONG_MAX || prefetchSize > SIZE_T_MAX || prefetchSize == 0)
					fatalInUserOptions("Prefetch size must be positive");

				OPTS.prefetchSize = (size_t)prefetchSize;
			break;
			case 'd':
				if(strlen(optarg) != 1)
					fatalInUserOptions("Field separator must be char");

				OPTS.keyFieldSeparator = *(char *)optarg;
			break;
			case '?':
			default:
				usage();
				exit(255);
		}
	}

	if(!strlen(filename)) {
		usage();
		exit(255);
	}

	if(OPTS.verbose)
		onAlarm(SIGALRM);

	setlinebuf(stdout);

	MDB_env *env;
	MDB_val kbuf, dbuf;
	int rc;
	int envflags = MDB_NOSUBDIR;
	
	dbuf.mv_size = 4096;
	dbuf.mv_data = malloc(dbuf.mv_size);

	rc = mdb_env_create(&env);
	if (rc) {
		fprintf(stderr, "mdb_env_create failed, error %d %s\n", rc, mdb_strerror(rc));
		return EXIT_FAILURE;
	}

	mdb_env_set_maxdbs(env, 2);
	mdb_env_set_mapsize(env, OPTS.map_size);

	rc = mdb_env_open(env, filename, envflags, 0664);
	if (rc) {
		fprintf(stderr, "mdb_env_open failed, error %d %s\n", rc, mdb_strerror(rc));
		goto env_close;
	}

	kbuf.mv_size = mdb_env_get_maxkeysize(env) * 2 + 2;
	kbuf.mv_data = malloc(kbuf.mv_size);

	mainLoop(env);

env_close:
	mdb_env_close(env);

	return EXIT_SUCCESS;
}

void mainLoop(MDB_env *env) {
	char *keyPtr;
	ssize_t keyLen;
	TokenReader reader(STDIN_FILENO);
	
	// lmdb
	int rc;
	MDB_txn *txn;
	MDB_cursor *mc;
	MDB_dbi dbi;
	int putflags = 0;

	char *linePtr = NULL;
	size_t  lineN = 0;
	ssize_t lineLen;

	OPTS.reader = &reader;

	signal(SIGHUP, onSignal);
	signal(SIGINT, onSignal);
	signal(SIGKILL, onSignal);
	signal(SIGPIPE, onSignal);
	signal(SIGTERM, onSignal);

	signal(SIGALRM, onAlarm);
	
	if(OPTS.checkMode == 1){
	}else{
		putflags |= MDB_NOOVERWRITE; //|MDB_NODUPDATA;
	}

	MDB_val key, value;
	int batch = 0;

	rc = mdb_txn_begin(env, NULL, 0, &txn);
	if (rc) {
		fprintf(stderr, "mdb_txn_begin failed, error %d %s\n", rc, mdb_strerror(rc));
		return;
	}

	rc = mdb_open(txn, NULL, MDB_CREATE, &dbi);
	if (rc) {
		fprintf(stderr, "mdb_open failed, error %d %s\n", rc, mdb_strerror(rc));
		goto txn_abort;
	}

	rc = mdb_cursor_open(txn, dbi, &mc);
	if (rc) {
		fprintf(stderr, "mdb_cursor_open failed, error %d %s\n", rc, mdb_strerror(rc));
		goto txn_abort;
	}

	while((lineLen = reader.readUpToDelimiter('\n', (void **)&linePtr))) {
		if(lineLen < 0)
			fatal("Unable to read line from stdin");

		lineN++;
		char *lineEnd = (linePtr + lineLen);

		if(OPTS.keyField == -1) {
			keyPtr = linePtr;
			keyLen = lineLen;
		} else { // need to cut key field
			keyPtr = linePtr;
			keyLen = -1;

			for(int curField = 0; curField < OPTS.keyField - 1; curField++) {
				keyPtr = (char *)memchr(keyPtr, OPTS.keyFieldSeparator, lineEnd - keyPtr);

				if(!keyPtr) {
					keyLen = 0;
					break;
				}

				keyPtr++;
			}

			if(keyLen) {
				char *keyEnd = (char *)memchr(keyPtr, OPTS.keyFieldSeparator, lineEnd - keyPtr);

				if(!keyEnd) { // последний филд в строке
					keyLen = lineEnd - keyPtr;
				} else {
					keyLen = keyEnd - keyPtr;
				}
			} else { // нужного филда нет в строке

			}
		}
		
		key.mv_data = keyPtr;
		key.mv_size = keyLen;
		
		int seen = 0;
		
		if(OPTS.checkMode == 1){
			rc = mdb_cursor_get(mc, &key, &value, MDB_NEXT);
			if(rc == MDB_NOTFOUND){
				seen = 0;
			}else if(rc){
				fprintf(stderr, "%s: line %" Z "d: txn_commit: %s\n",
					prog, lineN, mdb_strerror(rc));
				return;
			}else{
				seen = (int)*((char *)value.mv_data);
			}
		}else{
			char value_1 = 1;
			value.mv_data = &value_1;
			value.mv_size = sizeof(value_1);
			
			rc = mdb_cursor_put(mc, &key, &value, putflags);
			if (rc == MDB_KEYEXIST && putflags){
				seen = 1;
			}else if (rc) {
				fprintf(stderr, "mdb_cursor_put failed, error %d %s\n", rc, mdb_strerror(rc));
				goto txn_abort;
			}else{
				seen = 0;
			}
		}
		
		if(OPTS.invert == 1){
			seen = seen ? 0 : 1;
		}
		
		if(OPTS.format_seen){
			fprintf(stdout, "%zu %u\n", lineN, seen);
		}else{
			if(!seen){
				int ret = fwrite(linePtr, lineLen, 1, stdout);
				(void)ret;
			}
		}
		
		batch++;
		if (batch == 100) {
			rc = mdb_txn_commit(txn);
			if (rc) {
				fprintf(stderr, "%s: line %" Z "d: txn_commit: %s\n",
					prog, lineN, mdb_strerror(rc));
				return;
			}
			rc = mdb_txn_begin(env, NULL, 0, &txn);
			if (rc) {
				fprintf(stderr, "mdb_txn_begin failed, error %d %s\n", rc, mdb_strerror(rc));
				return;
			}
			rc = mdb_cursor_open(txn, dbi, &mc);
			if (rc) {
				fprintf(stderr, "mdb_cursor_open failed, error %d %s\n", rc, mdb_strerror(rc));
				goto txn_abort;
			}
			batch = 0;
		}
	}

	OPTS.reader = NULL;
	
	rc = mdb_txn_commit(txn);
	txn = NULL;
	if (rc) {
		fprintf(stderr, "%s: line %" Z "d: txn_commit: %s\n",
			prog, lineN, mdb_strerror(rc));
		return;
	}
	mdb_dbi_close(env, dbi);

txn_abort:
	mdb_txn_abort(txn);
}

void onSignal(int sig) {
	if(OPTS.reader)
		OPTS.reader->setEof();
}

void onAlarm(int sig) {
	static double lastCallTime = -1;
	static size_t lastCallLineNumber = -1;
	static double firstCallTime = gettimed();
	static size_t firstCallLineNumber = STAT.lineNumber;

	if(lastCallTime > 0) {
		fprintf(
			stderr,
			"\rSpeed [i/s]: %u avg, %u cur                  ",
			(unsigned int)((STAT.lineNumber - firstCallLineNumber) / (gettimed() - firstCallTime)),
			(unsigned int)((STAT.lineNumber - lastCallLineNumber) / (gettimed() - lastCallTime))
		);
	}

	lastCallLineNumber = STAT.lineNumber;
	lastCallTime = gettimed();
	alarm(1);
}

const char *getHost(const char *url, size_t len) {
	static char host[128];
	size_t hostLen = 0;
	int numSlashes = 0;
	size_t i;

	for(i=0; i < len && url[i]; i++) {
		if(numSlashes == 2) {
			if(url[i] == '/')
				break;
			host[hostLen] = url[i];
			hostLen++;
			if(hostLen >= sizeof(host) - 1)
				break;
		}

		if(url[i] == '/')
			numSlashes++;
	}

	host[hostLen] = 0;
	return host;
}

void usage() {
	fputs("Usage: uq [-ucv] [-f N] [-d C] [-b N] [-f S] [-p S] -t storage\n", stderr);
	fputs("  -t <path>: path to storage\n", stderr);
	fputs("  -c: force creation of storage\n", stderr);
	fputs("  -u: url mode\n", stderr);
	fputs("  -V: verbose\n", stderr);
	fputs("  -v: invert match\n", stderr);
	fputs("  -r: read-only mode\n", stderr);
	fputs("  -s: output writes '<lineN> <0/1>' for every input line\n", stderr);
	fputs("  -f <number>: select key field\n", stderr);
	fputs("  -d <char>: use given delimiter instead of TAB for field delimiter\n", stderr);
	fputs("  -b <number>: block size\n", stderr);
	fputs("  -f <size>: cache size\n", stderr);
	fputs("  -p <size>: buffer prefetch size\n", stderr);
	fputs("  -k <1|2|4|8|16>: key hash size\n", stderr);
	fputs("  -l <size>: lmdb map size\n", stderr);
}

const unsigned char *getHash(const char *string, int stringLen) {
	static unsigned char hashBuf[32];

	if(OPTS.urlMode) {
		const char *host = getHost(string, stringLen);
		MD5((const unsigned char *)host, strlen(host), hashBuf);
		MD5((const unsigned char *)string, stringLen, hashBuf + 3);
	} else {
		MD5((const unsigned char *)string, stringLen, hashBuf);
	}

	return hashBuf;
}

size_t parseSize(const char *str) {
	char mul[] = {'b', 'k', 'm', 'g', 't', 'p', 'e', 'z', 'y'};
	char *inv;
	unsigned long l = strtoul(str, &inv, 0);

	if(l == ULONG_MAX)
		return SIZE_T_MAX;

	if(*inv != '\0') {
		size_t i;
		bool founded = false;
		for(i=0; i<sizeof(mul); i++) {
			if(tolower(*inv) == mul[i]) {
				l <<= 10 * i;
				founded = true;
				break;
			}
		}

		if(!founded)
			return SIZE_T_MAX;

		if(*(inv + 1) != '\0' && tolower(*(inv + 1)) != 'b')
			return SIZE_T_MAX;
	}

	if(l > SIZE_T_MAX)
		return SIZE_T_MAX;

	return (size_t)l;
}

/* THE END */
