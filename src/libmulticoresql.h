/** @file
 */

#ifndef LIBMULTICORESQL_H
#define LIBMULTICORESQL_H

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

struct mu_SQLITE3_TASK {
  pid_t pid;
  int status;
  const char *dirname;
  const char *taskname;
  int tasknum;
  const char *iname;
  const char *oname;
  const char *ename;
  const char *pname;
};

struct mu_SQLITE3_TASK * mu_define_task(const char *dirname, const char *taskname, int tasknum);
int mu_start_task(struct mu_SQLITE3_TASK *task, const char *abortmsg);
int mu_finish_task(struct mu_SQLITE3_TASK *task, const char *abortmsg);
void mu_free_task(struct mu_SQLITE3_TASK *task);

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

int mu_create_shards_from_sqlite_table(const char *dbname, const char *tablename, const char *dbdir);

int mu_create_shards_from_csv(const char *csvname, int skip, const char *schemaname, const char *tablename, const char *dbDir, int shardc);

int mu_fLoadExtensions(FILE *f);


/** Database conf 

 */

struct mu_DBCONF {
  const char *db; /**< directory containg sqlite3 shards */
  const char *otablename; /**< table name for collecting data from map query */ 
  int isopen; /**< flag indicating database has been opened */
  int ncores; /**< number of simultaneous processes to run for queries */
  size_t shardc; /**< count of sqlite3 database shard files */
  const char **shardv; /**< file names of sqlite3 database shards  */
};

/** generate default database conf */
struct mu_DBCONF * mu_defaultconf();

/** open database directory */
int mu_opendb(struct mu_DBCONF * conf /**< [inout] database config */, 
	      const char *dbdir     /**< [in] /path/to/directory of sqlite3 shards */
	      );

/** create sqlite3 command file for map query processing */
int mu_makeQueryCoreFile3(struct mu_DBCONF * conf /**< [in] database conf */, 
			 const char *fname /**< [in] filename for sqlite3 command, to be created */, 
			 int getschema /**< [in] flag to cause autogeneration of table schema from map query output */, 
			 int shardc /**< [in] count of shards to act upon */, 
			 const char **shardv /**< [in] list of filenames for shards to act upon */, 
			 const char *mapsql /**< [in] sqlite3 query to execute in each of the shards */
			 );

/** run a map query, and optionally a reduce query against the shard collection in conf */
int mu_query3(struct mu_DBCONF *conf,
	  const char *mapsql,
	  const char *createtablesql,
	  const char *reducesql);


int mu_makeQueryCoreFile(struct mu_DBCONF * conf, const char *fname, const char *coredbname, int shardc, const char **shardv, const char *mapsql);

int mu_query(struct mu_DBCONF *conf,
	  const char *mapsql,
	  const char *createtablesql,
	  const char *reducesql);

#endif /* LIBMULTICORESQL_H */
