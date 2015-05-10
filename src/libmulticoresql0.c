/* libmulticoresql0.c (C) 2015 Paul Brewer 
 * Economic and Financial Technology Consulting LLC
 * All rights reserved. 
 * v0 is for student/home/lite edition
 * supports SELECT
 * maps run on 3 worker cores, reduce runs in 1 core
 * corefiles generated in an unrolled loop (anti-hack)
 * data pipelines are text files
 */

#include "libmulticoreutils.h"
#include "libmulticoresql0.h"

struct mu_CONF * mu_defaultconf(){
  typedef struct mu_CONF mu_conf_type;
  struct mu_CONF * c = mu_malloc(sizeof(mu_conf_type));
  c->bin = mu_sqlite3_bin();
  c->db = NULL;
  c->extensions = getenv("MULTICORE_SQLITE3_EXTENSIONS");
  c->otablename = "t";
  c->isopen = 0;
  c->ncores = 3;
  c->shardc = 0;
  c->shardv = NULL;
  return c;
}

int mu_opendb(struct mu_CONF * conf, const char *dbdir){
  conf->isopen=0;
  if (conf==NULL){
    fprintf(stderr,"%s\n","Fatal: mu_opendb received null mu_CONF pointer");
    exit(EXIT_FAILURE);
  }
  if (dbdir==NULL){
    fprintf(stderr,"%s\n","Fatal: mu_opendb received null dbdir pointer");
    exit(EXIT_FAILURE);
  }
  char *glob = mu_cat(dbdir,"/*");
  wordexp_t p;
  int flags = WRDE_NOCMD; // do not run commands in shells like "$(evil)"
  int x = wordexp(glob, &p, flags);
  if (x==0){
    conf->shardc = p.we_wordc;
    // fprintf(stderr,"found %zd shards \n",conf->shardc);
    conf->shardv = (const char **) p.we_wordv;
    // mark conf as open if return values make sense
    if ((conf->shardc>1) &&
	(conf->shardv[0]) &&
	(conf->shardv[conf->shardc]==NULL)
	) conf->isopen=1;
  }
  free(glob);
  return (conf->isopen)? 0: -1;
}

int mu_fLoadExtensions(struct mu_CONF *conf, FILE *f){
  if ( (conf->extensions) && (strlen(conf->extensions)>0) ){
    char *ext = mu_dup(conf->extensions);
    char *tok = strtok(ext, " ");
    while (tok){
      errno = 0;
      fprintf(f,".load %s\n",tok);
      tok = strtok(NULL," ");
    }
    free(ext); // safe?
  }
  return errno;
}

int mu_makeQueryCoreFile(struct mu_CONF * conf, const char *fname, int getschema, int shardc, const char **shardv, const char *mapsql){

  int i;

  const char *qTemplate =
    ".mode insert %s\n" // output table name
    ;

  const char *qData =
    "%s\n";
  
  const char *qSchema =
    "create temporary table %s as %s\n"
    ".schema %s \n"
    "select * from %s; \n";
  
  FILE * qcfile = mu_fopen(fname,"w");

  fprintf(qcfile,
	  qTemplate,
	  conf->otablename);
  
  for(i=0;i<shardc;++i){
    if (shardv[i]){
      fprintf(qcfile, ".open %s\n", shardv[i]);
      mu_fLoadExtensions(conf, qcfile);
      if ((getschema) && (i==0)){
	fprintf(qcfile,
		qSchema,
		conf->otablename,
		mapsql,
		conf->otablename,
		conf->otablename);	
      } else {
	fprintf(qcfile,qData,mapsql);
      }
    }
  }

  fclose(qcfile);
  
  return 0;
  
}

int mu_query(struct mu_CONF *conf,
	     const char *mapsql,
	     const char *createtablesql,
	     const char *reducesql)
{
  int icore;

  int status;

  size_t bufsize=255; // for constructing fnames
  
  pid_t worker[conf->ncores];

  errno = 0;
  const char *tmpdir = mu_create_temp_dir();
  mu_abortOnError("Fatal: unable to create temporary work directory");

  char reducefname[bufsize];
  FILE *reducef;

  char resultfname[bufsize];
  snprintf(resultfname, bufsize, "%s/result", tmpdir);
  FILE *resultf;

  if (reducesql){
    snprintf(reducefname, bufsize, "%s/query.reduce", tmpdir);
    reducef = mu_fopen(reducefname, "w");
    mu_fLoadExtensions(conf, reducef);
    if (createtablesql)
      fprintf(reducef, "%s\n", createtablesql);
    fprintf(reducef,"%s\n", "BEGIN TRANSACTION;");
  }
  
  char fname[bufsize], resultname[bufsize];
  int shardc;
  const char ** shardv;
  char * const argv[] = {"sqlite3", (char * const) NULL};
  
  /* create block macro */
#define RUN_CORE(mycore) \
  icore = mycore; \
  snprintf(fname, bufsize, "%s/query.sqlite.core.%.3d", tmpdir, icore); \
  snprintf(resultname,bufsize,"%s/results.core.%.3d",tmpdir,icore); \
  if (reducesql) \
    fprintf(reducef, ".read %s \n", resultname); \
  shardc = mu_getcoreshardc(icore, conf->ncores, conf->shardc); \
  shardv = (const char **) mu_getcoreshardv(icore, conf->ncores, conf->shardc, conf->shardv); \
  mu_makeQueryCoreFile(conf,  \
		       fname,						\
		       (((icore==0) && (reducesql!=NULL) && (createtablesql==NULL))? 1: 0), \
		       shardc,						\
		       shardv,						\
		       mapsql);						\
  errno = 0;								\
  worker[icore] = mu_run(conf->bin,argv,fname,resultname);		\
  free(shardv);								\
  
  /* end of macro */

  /* expand macro and run 3 workers */
  RUN_CORE(0);
  RUN_CORE(1);
  RUN_CORE(2);

  // wait for workers
  waitpid(worker[0], &status, 0);
  waitpid(worker[1], &status, 0);
  waitpid(worker[2], &status, 0);
  
  if (reducesql){
    fprintf(reducef,"%s\n", "COMMIT;");
    fprintf(reducef, "%s\n", reducesql);
    fclose(reducef);
    char * const argv[] = {"sqlite3", (char *const) NULL};
    int reducer = mu_run(conf->bin, argv, reducefname, resultfname);
    char *cmd = mu_cat("cat ",resultfname);
    waitpid(reducer, &status, 0);
    system(cmd);
    free(cmd);
  }

  return 0;

}


