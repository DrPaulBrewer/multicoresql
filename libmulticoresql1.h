#ifndef LIBMULTICORESQL_H
#define LIBMULTICORESQL_H


struct CONF {
  const char *bin;
  const char *db;
  const char *extensions;
  const char *otablename;
  int isopen;
  int ncores;
  size_t shardc;
  const char **shardv;
};

struct CONF * defaultconf();

void * xmalloc(size_t size);

char * xcat(const char *s1, const char *s2);

char * xdup(const char *s);

void abortOnError(const char *msg);

FILE* xfopen(const char *fname, const char *mode);

int xrmtmp(const char *dirname);

pid_t xrun(const char *bin, char * const* argv, const char *infile, const char *outfile);

int fLoadExtensions(struct CONF *conf, FILE *f);

int opendb(struct CONF * conf, const char *dbdir);

int makeQueryCoreFile(struct CONF * conf, const char *fname, const char *coredbname, int shardc, const char **shardv, const char *mapsql);

int getshardc(int i, int n, int c);

const char** getshardv(int i, int n, int c, const char ** v);

int query(struct CONF *conf,
	  const char *mapsql,
	  const char *createtablesql,
	  const char *reducesql);

#endif /* LIBMULTICORESQL_H */
