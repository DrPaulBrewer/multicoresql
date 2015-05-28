#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "libmulticoresql.h"

int main(int argc, char **argv){
  char *dbname = NULL;  /* -d */
  char *tablename = NULL;  /* -t */
  char *mapsql = NULL; /* -m */
  char *reducesql = NULL; /* -r */
  int verbose = 0; /* -v */
  int ncores = 0; /* -c */

  const char *getopt_options = "c:d:t:m:r:v";
  int c;

  opterr = 1;
  
  while ((c = getopt(argc, argv, getopt_options)) != -1)
    switch(c)
      {
      case 'c':
	errno = 0;
	ncores = (int) strtol(optarg,NULL,10);
	if (errno==0)
	  break;
	fprintf(stderr,"Option -c requires number, got %s \n", optarg);
	return 1;
      case 'd':
	dbname = optarg;
	break;
      case 't':
	tablename = optarg;
	break;
      case 'm':
	mapsql = optarg;
	break;
      case 'r':
	reducesql = optarg;
	break;
      case 'v':
	verbose = 1;
	break;	
      case '?':
	if (strchr(getopt_options,c))
	  fprintf(stderr,"Option -%c missing valid setting\n", optopt);
	else if (isprint(optopt))
	  fprintf(stderr,"Unknown option -%c \n",optopt);
	else
	  fprintf(stderr, "Unknown option character");
	return 1;
      default:
	abort();
      }

  struct mu_DBCONF * conf = NULL;

  if ( (conf = mu_opendb(dbname)) != NULL){
    if (ncores)
      conf->ncores = ncores;
    if (verbose){
      fprintf(stdout,"sqls \n");
      fprintf(stdout,"number of cores (-c): %d\n",conf->ncores); 
      if (dbname) fprintf(stdout,"dbname              : %s \n",dbname);
      if (tablename) fprintf(stdout,"tablename           : %s \n",tablename);
      if (mapsql) fprintf(stdout,"mapsql:\n%s\n",mapsql);
      if (reducesql) fprintf(stdout,"reducesql:\n%s\n",reducesql);
    }
    int qstatus =  mu_query(conf, mapsql, NULL, reducesql);
    const char *qerror = mu_error_string();
    if (qerror)
      fputs(qerror, stderr);
    return qstatus;
  } else {
    fprintf(stderr, "error opening database %s \n",dbname);
  }
}
