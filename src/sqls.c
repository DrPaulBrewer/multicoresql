/* sqls.c 
   Copyright 2015 Paul Brewer <drpaulbrewer@eaftc.com> Economic and Financial Technology Consulting LLC
   License:  MIT
   Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 documentation files (the "Software"), to deal in the Software without restriction, including without limitation 
the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and 
to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO 
THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS 
OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

*/


#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "multicoresql.h"

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
	ncores = (int) strtol(optarg,NULL,10);
	if (ncores>0) break;
	fprintf(stderr,"Option -c requires positive number, got %s \n", optarg);
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
    char *qresult =  mu_run_query(conf,
				  mu_create_query(mapsql, NULL, reducesql)
				  );
    if (qresult)
      fputs(qresult, stdout);
    const char *qerror = mu_error_string();
    if (qerror)
      fputs(qerror, stderr);
  } else {
    fprintf(stderr, "error opening database %s \n",dbname);
  }
}
