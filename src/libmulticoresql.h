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
#include <time.h>

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

const char *mu_error_string();
void mu_error_clear();

char * mu_read_small_file(const char *fname);

int mu_create_shards_from_sqlite_table(const char *dbname, const char *tablename, const char *dbdir);

int mu_create_shards_from_csv(const char *csvname, int skip, const char *schemaname, const char *tablename, const char *dbDir, int shardc);

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

/** open database directory */
struct mu_DBCONF * mu_opendb(
	      const char *dbdir     /**< [in] /path/to/directory of sqlite3 shards */
	      );

struct mu_QUERY {
  const char *mapsql; /**< REQUIRED sqlite command(s)/statement(s) to map over shards */
  const char *createtablesql; /**< OPTIONAL sqlite CREATE TABLE statement to create the table format used to hold collected mapsql results.  You should name this table "maptable". i.e. "create table maptable ( blah, blah, blah );"  */
  const char *reducesql; /**< OPTIONAL sqlite statements to apply against the results collected in maptable from running the mapsql statement in all shards.  Necessary for reducing the collected mapsql results down to a final answer.  */
};

/** reads sql commands from strings or files and packs into a new query object */
struct mu_QUERY  * mu_create_query(const char *mapsql_or_fname,
				 const char *createtablesql_or_fname,
				 const char *reducesql_or_fname);

/** run a map query, and optionally a reduce query against the shard collection in conf */
char * mu_run_query(struct mu_DBCONF *conf, struct mu_QUERY *q);

#endif /* LIBMULTICORESQL_H */
