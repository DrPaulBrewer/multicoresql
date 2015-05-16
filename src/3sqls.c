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
	  fprintf(stderr,"Error: 3sqls is confused by your command.  Option -%c needs a setting, but I don't see it. If the setting is more than one word, remember to put quotes around it. Please fix this and try again.  \n", optopt);
	else if (isprint(optopt))
	  fprintf(stderr,"Error:  3sqls is confused by your command. I don't know what to do with - %c . Please remove this part or use the right option and try again.  \n",optopt);
	else
	  fprintf(stderr, "%s\n", "Error: 3sqls is confused by your command because it contains characters that I did not expect to see. Please remove any non-ascii characters from your command and try again.");
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

  if (dbname==NULL){
    fprintf(stderr,"%s\n","Error: Missing info. 3sqls can't read your mind.  Please tell it what database directory to use. Add -d dbdir to your command and try again");
    exit(EXIT_FAILURE);
  }

  if (mapsql==NULL){
    fprintf(stderr,"%s\n", "Error: Missing info. 3sqls can't read your mind. Please tell it what sql statments to execute (map) across the databases in the database directory.  Add -m \"statement1; statement2; statementN;\" or -m \"$(cat m.sql)\" to your command and try again. ");
    exit(EXIT_FAILURE);
  }
  
  if (mu_opendb(conf, dbname)==0)
    return mu_query(conf, mapsql, NULL, reducesql);
  else
    fprintf(stderr, "Error: Couldn't read the database at  %s \n",dbname);
}
