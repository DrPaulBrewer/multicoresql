/* libmulticoreutils.c Copyright 2015 Paul Brewer
 * Economic and Financial Technology Consulting LLC
 * All rights reserved.
 * common routines for all versions of multicoresql
 */

#include "libmulticoreutils.h"

void * xmalloc(size_t size){
  void * p = malloc(size);
  if (p==NULL){
    fprintf(stderr,"%s\n","Fatal error: out of memory\n");
    exit(1);
  }
  return p;
}

char * xcat(const char *s1, const char *s2){
  char * s = xmalloc(snprintf(NULL, 0, "%s%s", s1, s2) + 1);
  sprintf(s,"%s%s",s1,s2);
  return s;
}

char * xdup(const char *s){
  errno = 0;
  char *x = strdup(s);
  if ((x==NULL) || (errno!=0)){
    perror("out of memory");
    exit(1);
  }
  return x;
}

void abortOnError(const char *msg){
  if (errno!=0){
    perror(msg);
    exit(1);
  } 
}

FILE* xfopen(const char *fname, const char *mode){
  errno = 0;
  FILE *f = fopen(fname, mode);
  abortOnError(xcat("Fatal error on opening file ",fname));
  return f;
}

int xrmtmp(const char *dirname){
  if ((dirname) && (dirname==strstr(dirname, "/tmp/")))
    {
      char *fmt = "rm -rf %s";
      int len = snprintf(NULL, 0, fmt, dirname);
      char *cmd = xmalloc(len);
      sprintf(cmd, fmt, dirname);
      errno = 0;
      system(cmd);
      abortOnError(xcat("Fatal error removing directory ",dirname));
      free(cmd);
      return 0;
    }
  fprintf(stderr, "warning: attempt to rm -rf %s; not executed", dirname);
  return -1;
}

pid_t xrun(const char *bin, char * const* argv, const char *infile, const char *outfile){
  errno = 0;
  pid_t pid = fork();
  if (pid>0) return pid;
  if (pid==0){
    errno = 0;
    int rd, wd;
    if (infile) {
      rd = open(infile, O_RDONLY);
      if (errno!=0){
	perror(xcat("Fatal: xrun could not open input file ",infile));
	exit(EXIT_FAILURE);
      }
      dup2(rd,0);
      if (errno!=0){
	perror("Fatal: xrun could not set input file with dup2 ");
	exit(EXIT_FAILURE);
      }
    }
    if (outfile){
      wd = open(outfile, O_WRONLY | O_CREAT, 0700);
      if (errno!=0){
	perror(xcat("Fatal: xrun could not open output file ",outfile));
	exit(EXIT_FAILURE);
      }
      dup2(wd,1);
      if (errno!=0){
	perror("Fatal: xrun could not set output file with dup2 ");
	exit(EXIT_FAILURE);
      }
    }
    int err = execvp(bin, argv);
    if ((err!=0) || (errno!=0)){
      perror("Fatal: execvp failed ");
      exit(EXIT_FAILURE);
    }
  } 
  perror("Fatal: failed to fork and run command ");
  exit(EXIT_FAILURE);
}

int getcoreshardc(int i, int n, int c){
  int each = c/n;
  int extras = (i<(c%n))? 1: 0;
  return each+extras;
}

const char ** getcoreshardv(int i, int n, int c, const char ** v){
  char *x=NULL;
  int shardc = getcoreshardc(i,n,c);
  const char ** result = (const char **) xmalloc(shardc*sizeof(x));
  int idx;
  for(idx=0; (idx*n+i)<c; ++idx){
    result[idx] = v[(idx*n+i)];
  }
  return result;
}

