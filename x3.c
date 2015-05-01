#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>


struct CONF {
  char *bin;
  char *db;
  char *extensions;
  char *otablename;
  int ncores;
};

void * xmalloc(size_t size){
  void * p = malloc(size);
  if (p==NULL){
    fprintf(stderr,"%s\n","Fatal error: out of memory\n");
    exit(1);
  }
}

char * xcat(char *s1,char *s2){
  char * s = xmalloc(snprintf(NULL, 0, "%s %s", s1, s2) + 1);
  sprintf(s, "%s %s", s1, s2);
  return s;
}

void abortOnError(char *msg){
  if (errno!=0){
    perror(msg);
    exit(1);
  } 
}

FILE* xfopen(char *fname, const char *mode){
  errno = 0;
  FILE *f = fopen(fname, mode);
  abortOnError(xcat("Fatal error on opening file ",fname));
  return f;
}

int makeQueryCoreFile(struct CONF * conf, char *fname,int ntables,char **tablenames, int makeschema, char *mapsql){

  int i;

  const char *qTemplate =
    "#!/bin/bash\n"
    "%s <<EOF"
    ".mode insert %s\n" // output table name
    ".open %s\n"        // input db name
    ;

  
  const char *default_sqlite3bin = "sqlite3";

  FILE * qcfile = xfopen(fname,"w");

  char *sql = xcat(mapsql,";\n");

  char *sqlfmt;
  
  if (makeschema){
    const char *fmt = "create table %s as %s";
    sqlfmt = xmalloc(snprintf(NULL,0,fmt,conf->otablename,sql));
    sprintf(sqlfmt,fmt,conf->otablename,sql);

  } else {
    sqlfmt = xmalloc(strlen(sql)+1);
    strcpy(sqlfmt, sql);
  }

  free(sql);
  
  fprintf(qcfile,qTemplate,(conf->sqlite3bin? conf->sqlite3bin: default_sqlite3bin),conf->otablename,conf->dbname);
  
  if ( (conf->extensions) && (strlen(conf->extensions)>0) ){
    char *ext = xmalloc(strlen(conf->extensions)+1);
    strcpy(ext, conf->extensions);
    char *tok = strtok(ext, " ");
    while (tok){
      fprintf(qcfile,".load %s\n",tok);
      tok = strtok(NULL," ");
    }
    free(ext); // safe?
  }
  
  for(i=0;i<ntables;++i)
    fprintf(qcfile, sqlfmt, tablenames[i]);
  
  free(sqlfmt);

  if (makeschema){
    const char *fmt = 
      ".schema %s ; \n"
      "select * from %s; \n";
    fprintf(qcfile, fmt, conf->otablename, conf->otablename);
  }

  fprintf(qcfile,"%s\n","EOF");

  fchmod(qcfile, 0700);
  fclose(qcfile);
  
  return 0;
  
}

int query(struct CONF *conf, char *tablename, int nsplits, char *mapsql, char *reducesql){
  int icore;

  int pid[conf->ncores];

  for(icore=0;icore<conf->ncores;++icode){
    size_t bufsize=100;
    char fname[bufsize];
    makeQueryCoreFile(conf,fname,ntables,tablenames,icore==0,mapsql);
    pid[icore] = runQueryCoreFile(fname);
  }
  
  
  
  

}

int main(int argc, char **argv){
  long int ncores = 4;      /* -c */
  char *dbname = NULL;  /* -d */
  char *tablename = NULL;  /* -t */
  char *mapsql = NULL; /* -m */
  char *reducesql = NULL; /* -r */
  int verbose = 0; /* -v */
  

  struct CONF conf;

  conf.bin = getenv("MULTICORE_SQLITE3");
  conf.extensions = getenv("MULTICORE_SQLITE3_EXTENSIONS");
  
  

  const char *getopt_options = "c:d:t:m:r:v";
  int c;

  opterr = 1;
  
  while ((c = getopt(argc, argv, getopt_options)) != -1)
    switch(c)
      {
      case 'c':
	errno = 0;
	ncores = strtol(optarg,NULL,10);
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

  conf.ncores = ncores;
  conf.otablename = "t";

  if (verbose){
    fprintf(stdout,"x3 \n");
    fprintf(stdout,"number of cores (-c): %li \n",ncores); 
    if (dbname) fprintf(stdout,"dbname              : %s \n",dbname);
    if (tablename) fprintf(stdout,"tablename           : %s \n",tablename);
    if (mapsql) fprintf(stdout,"mapsql:\n%s\n",mapsql);
    if (reducesql) fprintf(stdout,"reducesql:\n%s\n",reducesql);
  }


  return query(&conf, tablename, TODOnsplits, mapsql, reducesql)

}
