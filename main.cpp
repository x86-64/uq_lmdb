#include <stdlib.h>
#include <stdio.h>
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
#include <ctype.h>

#include "btree.hpp"
#include "misc.hpp"

struct options {
	size_t blockSize;
	char forceCreateNewDb;
	char urlMode;
	char preSort;
	size_t preSortBufferSize;
	
	// fields control
	int  fields_enabled;
	int  choose_field;
	char field_sep;
	int  ignore_case;
};
/* ------------------------------------- */

static struct options OPTS;

void usage();
const char *getHost(const char *url);
const unsigned char *getHash(const char *string, int string_len);
int getStdinLine(char *buf, int buf_size, char **line_start, int *line_len);

int main(int argc, char *argv[]) {
	const char *filename = "";
	unsigned long blockSize;
	unsigned long preSortBufferSize;

// 	size_t preSortBufferCurrentSize = 0;

	char ch;

	OPTS.blockSize = 4096*2;
	OPTS.forceCreateNewDb = 0;
	OPTS.urlMode = 0;
	OPTS.preSort = 0;
	OPTS.preSortBufferSize = 256;
	// fields control
	OPTS.fields_enabled = 0;
	OPTS.choose_field   = 0;
	OPTS.field_sep      = ';';
	OPTS.ignore_case    = 0;
	
	while ((ch = getopt(argc, argv, "scuib:t:S:f:d:")) != -1) {
		switch (ch) {
			case 'b':
				blockSize = strtoul(optarg, NULL, 0);
				if(blockSize < 32 || blockSize == ULONG_MAX) {
					fputs("Block size must be >32\n", stderr);
					exit(255);
				}
				OPTS.blockSize = blockSize;
			break;
			case 't':
				filename = optarg;
			break;
			case 'c':
				OPTS.forceCreateNewDb = 1;
			break;
			case 's':
				OPTS.preSort = 1;
			break;
			case 'S':
				preSortBufferSize = strtoul(optarg, NULL, 0);
				if(preSortBufferSize < 32 || preSortBufferSize == ULONG_MAX) {
					fputs("Block size must be >32\n", stderr);
					exit(255);
				}
				OPTS.preSortBufferSize = preSortBufferSize;
			break;
			case 'u':
				OPTS.urlMode = 1;
			break;
			case 'f':
				OPTS.fields_enabled = 1;
				OPTS.choose_field   = strtoul(optarg, NULL, 0);
			break;
			case 'd':
				OPTS.field_sep      = *(char *)optarg;
			break;
			case 'i':
				OPTS.ignore_case    = 1;
			break;
			case '?':
			default:
				usage();
				exit(255);
		}
	}

// 	struct rlimit rl;
// 	if(getrlimit(RLIMIT_MEMLOCK, &rl) == 0) {
// 		if(rl.rlim_cur == RLIM_INFINITY)
// 			exit(0);
// 		printf("RLIMIT_MEMLOCK: %llu; max: %llu\n", (unsigned long long)rl.rlim_cur, (unsigned long long)rl.rlim_max);
// 	}

	if(!strlen(filename)) {
		usage();
		exit(255);
	}

	UniqueBTree tree(filename);

	if(access(filename, R_OK | W_OK) == 0 && !OPTS.forceCreateNewDb) {
		tree.load();
		fprintf(stderr, "Btree from %s with blockSize=%u was loaded\n", filename, tree.blockSize);
	} else {
		tree.create(OPTS.blockSize);
		fprintf(stderr, "New btree in %s with blockSize=%u was created\n", filename, tree.blockSize);
	}
/*
	for(unsigned int i=0; i<1400; i++) {
		tree.add(MD5((unsigned char *)&i, sizeof(i), NULL));
	}*/
	setlinebuf(stdin);
	setlinebuf(stdout);
	
	char line[1024];
	char *line_ptr;
	int   line_len;
	
	if(OPTS.preSort) {
		char *preSortBuffer;
		size_t preSortBufferCurrentSize = 0;
		size_t i;
		size_t itemSize = 8 + sizeof(void *);

		if(!(preSortBuffer = (char *)calloc(OPTS.preSortBufferSize, itemSize))) {
			perror("calloc");
			exit(errno);
		}

		while(1) {
			char newItem[8 + sizeof(void *)];
			const unsigned char *hash;
			off_t index;

			if(preSortBufferCurrentSize >= OPTS.preSortBufferSize) {
				char *popedItem = preSortBuffer + (preSortBufferCurrentSize - 1) * itemSize;
				char *lineS;
				char **ptr;

				ptr = (char **)(popedItem + 8);
				lineS = *ptr;

				if(tree.add(popedItem))
					fputs(lineS, stdout);

				free(lineS);
				preSortBufferCurrentSize--;
			}

			if(getStdinLine(line, sizeof(line), &line_ptr, &line_len) == 0)
				break;
			
			hash = getHash(line_ptr, line_len);
			
			memcpy(newItem, hash, 8);
			
			if(preSortBufferCurrentSize) {
				index = insertInSortedArray(preSortBuffer, itemSize, preSortBufferCurrentSize, newItem);
			} else {
				memcpy(preSortBuffer, newItem, itemSize);
				index = 0;
			}
			if(index != -1) {
				char *t = preSortBuffer + index * itemSize;
				char *lineS = (char *)malloc(strlen(line) + 1);
				char **ptr = (char **)(t + 8);
				strcpy(lineS, line);
				*ptr = lineS;

				preSortBufferCurrentSize++;
			}
		}

		for(i=0; i<preSortBufferCurrentSize; i++) {
			char *t = preSortBuffer + i * itemSize;
			char **ptr = (char **)(t + 8);
			char *lineS = *ptr;

			if(tree.add(t))
				fputs(lineS, stdout);
			free(lineS);
		}
		preSortBufferCurrentSize = 0;

	} else {
		while(getStdinLine(line, sizeof(line), &line_ptr, &line_len)) {
			if(tree.add(getHash(line_ptr, line_len)))
				fputs(line, stdout);
		}
	}

	return EXIT_SUCCESS;
}

void strtolower(char *str, int len){
	int i;
	
	if(OPTS.ignore_case == 0)
		return;

	for(i=0; i<len; i++){
		*str = tolower(*str);
	}
}

// returns 0 on EOF, 1 on success
int getStdinLine(char *buf, int buf_size, char **line_start, int *line_len){
	int eol, curr_field;
	char *curr, *next;
	
	do{
		if(!fgets(buf, buf_size, stdin))
			return 0;
		
		if(OPTS.fields_enabled == 0){
			*line_start = buf;
			*line_len   = strlen(buf);
			strtolower(*line_start, *line_len);
			return 1;
		}
		
		curr = buf;
		eol  = 0;
		curr_field = 1;
		do{
			next = strchr(curr, OPTS.field_sep);
			if(next == NULL){
				if(curr_field == OPTS.choose_field){
					*line_start = curr;
					*line_len   = strlen(curr) - 1;
					strtolower(*line_start, *line_len);
					return 1;
				}
				break;
			}
			
			if(curr_field == OPTS.choose_field){
				*line_start = curr;
				*line_len   = next - curr;
				strtolower(*line_start, *line_len);
				return 1;
			}
			curr = next + 1; // skip field sep
			curr_field++;
		}while(curr);
	}while(1);
}

const char *getHost(const char *url) {
	static char host[128];
	size_t hostLen = 0;
	int numSlashes = 0;
	const char *chr;

	for(chr=url; *chr; chr++) {
		if(numSlashes == 2) {
			if(*chr == '/')
				break;
			host[hostLen] = *chr;
			hostLen++;
			if(hostLen >= sizeof(host) - 1)
				break;
		}
		
		if(*chr == '/')
			numSlashes++;
	}
	host[hostLen] = 0;
	return host;
}

void usage() {
	fputs("Usage: uniq [-uc] [-S bufSize] [-b blockSize] -t btreeFile\n", stderr);
	fputs("\n", stderr);
	fputs("  -u        url mode\n", stderr);
	fputs("  -s        pre-sort input\n", stderr);
	fputs("  -S        pre-sort buffer size\n", stderr);
	fputs("  -f        select field\n", stderr);
	fputs("  -d        use given delimiter instead of ';'\n", stderr);
	fputs("  -i        ignore case\n", stderr);
}

const unsigned char *getHash(const char *string, int string_len) {
	static unsigned char hashBuf[32];
	
	if(OPTS.urlMode) {
		const char *host = getHost(string);
		MD5((const unsigned char *)host, strlen(host), hashBuf);
		MD5((const unsigned char *)string, string_len, hashBuf+3);
	} else {
		MD5((const unsigned char *)string, string_len, hashBuf);
	}
	return hashBuf;
}

/* THE END */
