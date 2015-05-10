#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "libmulticoresql0.h"

int main(int argc, char **argv){
  char *dbname = NULL;  /* -d */
  char *tablename = NULL;  /* -t */
  char *mapsql = NULL; /* -m */
  char *reducesql = NULL; /* -r */
  int verbose = 0; /* -v */

  struct mu_CONF *conf = mu_defaultconf();

  const char *getopt_options = "d:t:m:r:v";
  int c;

  opterr = 1;
  
  while ((c = getopt(argc, argv, getopt_options)) != -1)
    switch(c)
      {
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

  if (verbose){
    fprintf(stdout,"x3 \n");
    fprintf(stdout,"number of cores (-c): %d\n",conf->ncores); 
    if (dbname) fprintf(stdout,"dbname              : %s \n",dbname);
    if (tablename) fprintf(stdout,"tablename           : %s \n",tablename);
    if (mapsql) fprintf(stdout,"mapsql:\n%s\n",mapsql);
    if (reducesql) fprintf(stdout,"reducesql:\n%s\n",reducesql);
  }

  if (mu_opendb(conf, dbname)==0)
    return mu_query(conf, mapsql, NULL, reducesql);
  else
    fprintf(stderr, "error opening database %s \n",dbname);
}
