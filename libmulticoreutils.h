#ifndef LIBMULTICOREUTILS_H
#define LIBMULTICOREUTILS_H

#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <wordexp.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <fcntl.h>


void * xmalloc(size_t size);

char * xcat(const char *s1, const char *s2);

char * xdup(const char *s);

void abortOnError(const char *msg);

FILE* xfopen(const char *fname, const char *mode);

int xrmtmp(const char *dirname);

pid_t xrun(const char *bin, char * const* argv, const char *infile, const char *outfile);

int getcoreshardc(int i, int n, int c);

const char ** getcoreshardv(int i, int n, int c, const char ** v);

#endif /* LIBMULTICOREUTILS_H */
