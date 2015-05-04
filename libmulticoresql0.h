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

int fLoadExtensions(struct CONF *conf, FILE *f);

int opendb(struct CONF * conf, const char *dbdir);

int makeQueryCoreFile(struct CONF * conf, const char *fname, int getschema, int shardc, const char **shardv, const char *mapsql);

int query(struct CONF *conf,
	  const char *mapsql,
	  const char *createtablesql,
	  const char *reducesql);

#endif /* LIBMULTICORESQL_H */
