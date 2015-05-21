/** @file
*/

#ifndef LIBMULTICORESQL_H
#define LIBMULTICORESQL_H

/** Database conf 

 */

struct mu_CONF {
  const char *bin; /**< sqlite3 binary location */
  const char *db; /**< directory containg sqlite3 shards */
  const char *otablename; /**< table name for collecting data from map query */ 
  int isopen; /**< flag indicating database has been opened */
  int ncores; /**< number of simultaneous processes to run for queries */
  size_t shardc; /**< count of sqlite3 database shard files */
  const char **shardv; /**< file names of sqlite3 database shards  */
};

/** generate default database conf */
struct mu_CONF * mu_defaultconf();

/** open database directory */
int mu_opendb(struct mu_CONF * conf /**< [inout] database config */, 
	      const char *dbdir     /**< [in] /path/to/directory of sqlite3 shards */
	      );

/** create sqlite3 command file for map query processing */
int mu_makeQueryCoreFile(struct mu_CONF * conf /**< [in] database conf */, 
			 const char *fname /**< [in] filename for sqlite3 command, to be created */, 
			 int getschema /**< [in] flag to cause autogeneration of table schema from map query output */, 
			 int shardc /** [in] count of shards to act upon */, 
			 const char **shardv /** [in] list of filenames for shards to act upon */, 
			 const char *mapsql /** [in] sqlite3 query to execute in each of the shards */
			 );

/** run a map query, and optionally a reduce query against the shard collection in conf */
int mu_query(struct mu_CONF *conf,
	  const char *mapsql,
	  const char *createtablesql,
	  const char *reducesql);

#endif /* LIBMULTICORESQL_H */
