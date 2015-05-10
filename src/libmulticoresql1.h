#ifndef LIBMULTICORESQL_H
#define LIBMULTICORESQL_H

struct mu_CONF {
  const char *bin;
  const char *db;
  const char *extensions;
  const char *otablename;
  int isopen;
  int ncores;
  size_t shardc;
  const char **shardv;
};

struct mu_CONF * mu_defaultconf();

int mu_fLoadExtensions(struct mu_CONF *conf, FILE *f);

int mu_opendb(struct mu_CONF * conf, const char *dbdir);

int mu_makeQueryCoreFile(struct mu_CONF * conf, const char *fname, const char *coredbname, int shardc, const char **shardv, const char *mapsql);

int mu_query(struct mu_CONF *conf,
	  const char *mapsql,
	  const char *createtablesql,
	  const char *reducesql);

#endif /* LIBMULTICORESQL_H */
