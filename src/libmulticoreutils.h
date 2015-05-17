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

void * mu_malloc(size_t size);

char * mu_cat(const char *s1, const char *s2);

char * mu_dup(const char *s);

void mu_abortOnError(const char *msg);

FILE* mu_fopen(const char *fname, const char *mode);

pid_t mu_run(const char *bin, char * const* argv, const char *infile, const char *outfile, const char *errfile);

int mu_warn_run_status(const char *msg, int status, const char *bin, const char *iname, const char *oname, const char *ename);

int mu_getcoreshardc(int i, int n, int c);

const char ** mu_getcoreshardv(int i, int n, int c, const char ** v);

const char * mu_create_temp_dir();

int mu_remove_temp_dir(const char *dirname);

int is_mu_temp(const char *fname);

int exists_mu_temp_file(const char *fname);

int exists_mu_temp_done(const char *dirname);

int mu_mark_temp_done(const char *dirname);

char * mu_read_small_file(const char *fname);

int ok_mu_shard_name(const char *name);

long int mu_get_long_int_or_die(const char *instring, const char *errfmt);

int mu_create_shards_from_sqlite_table(const char *dbname, const char *tablename, const char *dbdir);

int mu_create_shards_from_csv(const char *csvname, int skip, const char *schemaname, const char *tablename, const char *dbDir, int shardc);

const char * mu_sqlite3_bin();

int mu_fLoadExtensions(FILE *f);

#endif /* LIBMULTICOREUTILS_H */
