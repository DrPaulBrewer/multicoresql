#include "libmulticoreutils.h"
#include "libmulticoresql1.h"

struct mu_CONF * mu_defaultconf(){
  typedef struct mu_CONF conftype;
  struct mu_CONF * c = mu_malloc(sizeof(conftype));
  c->bin = getenv("MULTICORE_SQLITE3_BIN");
  if (c->bin==NULL)
    c->bin = "sqlite3";
  c->db = NULL;
  c->otablename = "t";
  c->isopen = 0;
  c->ncores = (int) sysconf(_SC_NPROCESSORS_ONLN);
  if ((c->ncores<=0) || (c->ncores>255)){
    fprintf(stderr,"Fatal error: sysconf returned invalid number of cores = %d , expected a number between 1 and 255 \n", c->ncores);
    exit(EXIT_FAILURE);
  }
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
  char *glob = mu_cat(dbdir,"/[0-9]*");
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



int mu_makeQueryCoreFile(struct mu_CONF * conf, const char *fname, const char *coredbname, int shardc, const char **shardv, const char *mapsql){

  int i;

  if ( (conf==NULL) || (fname==NULL) || (shardc==0) || (shardv==NULL) || (mapsql==NULL) || strlen(mapsql)==0 )
    return -1;

  FILE * qcfile = mu_fopen(fname,"w");

  int is_select = ( (mapsql[0]=='s') || (mapsql[0]=='S') );

  for(i=0;i<shardc;++i){
    if (shardv[i]){
      fprintf(qcfile, ".open %s\n", shardv[i]);
      mu_fLoadExtensions(qcfile);
      if (is_select){
	fprintf(qcfile, "attach database '%s' as 'resultdb';\n", coredbname); 
	fprintf(qcfile, "%s;\n", "pragma resultdb.synchronous = 0");
	if (i==0){
	  fprintf(qcfile,
		  "create table resultdb.%s as %s\n",
		  conf->otablename,
		  mapsql);	
	} else {
	  fprintf(qcfile,
		  "insert into resultdb.%s %s\n",
		  conf->otablename,
		  mapsql);
	}
      } else {
	/* mapsql is not a select statment */
	fprintf(qcfile,
		"%s\n",
		mapsql);
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

  errno=0;
  const char *tmpdir = mu_create_temp_dir();
  mu_abortOnError(mu_cat("Fatal: unable to create temporary work directory ", tmpdir));

  char reducefname[bufsize];
  FILE *reducef;
  char resultfname[bufsize];
  char rederrfname[bufsize];
  
  snprintf(resultfname, bufsize, "%s/result", tmpdir);
  FILE *resultf;
  
  if (reducesql){
    snprintf(reducefname, bufsize, "%s/query.reduce", tmpdir);
    snprintf(rederrfname, bufsize, "%s/query.reduce.err", tmpdir);
    reducef = mu_fopen(reducefname, "w");
    fprintf(reducef, ".open %s/coredb.000\n", tmpdir);
    fprintf(reducef, "%s;\n", "pragma synchronous = 0"); 
    mu_fLoadExtensions(reducef);
  }

  char * iname[conf->ncores];
  char * oname[conf->ncores];
  char * ename[conf->ncores];
  char * coredbname[conf->ncores];
  
  for(icore=0;icore<(conf->ncores);++icore){
    iname[icore] = mu_malloc(bufsize);
    oname[icore] = mu_malloc(bufsize);
    ename[icore] = mu_malloc(bufsize);
    coredbname[icore] = mu_malloc(bufsize);
    snprintf(iname[icore], bufsize, "%s/query.sqlite.core.%.3d", tmpdir, icore);
    snprintf(oname[icore], bufsize, "%s/results.core.%.3d", tmpdir, icore);
    snprintf(ename[icore], bufsize, "%s/errors.sqlite.core.%.3d", tmpdir, icore);
    snprintf(coredbname[icore],bufsize,"%s/coredb.%.3d",tmpdir,icore);
    if ((reducef) && (icore>0)){
      fprintf(reducef, "attach database '%s' as 'coredb%.3d';\n", coredbname[icore], icore);
      fprintf(reducef, "insert into %s select * from coredb%.3d.%s;\n", conf->otablename, icore, conf->otablename);
      fprintf(reducef, "detach database 'coredb%.3d';\n", icore);
    }
    int shardc = mu_getcoreshardc(icore, conf->ncores, conf->shardc);
    const char **shardv = mu_getcoreshardv(icore, conf->ncores, conf->shardc, conf->shardv);
    mu_makeQueryCoreFile(conf, iname[icore], coredbname[icore], shardc, shardv, mapsql);
    errno = 0;
    char * const argv[] = {"sqlite3", (char * const) NULL};
    worker[icore] = mu_run(conf->bin,argv,iname[icore],oname[icore],ename[icore]);
    free(shardv);
  }

  // wait for workers

  int worker_status[conf->ncores];
  
  for(icore=0;icore<conf->ncores;++icore){
    worker_status[icore] = 0;
    waitpid(worker[icore],worker_status+icore,0);
  }

  for(icore=0;icore<conf->ncores;++icore){
    char warnstr[32];
    snprintf(warnstr,32,"map query worker %.3d",icore);
    if (mu_warn_run_status(warnstr,worker_status[icore],conf->bin,iname[icore],oname[icore],ename[icore]))
      return -1;
    free( (void *) iname[icore]);
    free( (void *) oname[icore]);
    free( (void *) ename[icore]);
    free( (void *) coredbname[icore]);
  }
  
  if (reducesql){
    int reducer_status=0;
    fprintf(reducef, "%s\n", reducesql);
    fclose(reducef);
    char * const argv[] = {"sqlite3", (char *const) NULL};
    int reducer = mu_run(conf->bin, argv, reducefname, resultfname, rederrfname);
    waitpid(reducer, &reducer_status, 0);
    if (mu_warn_run_status("reduce query worker ",
			   reducer_status,
			   conf->bin,
			   reducefname,
			   resultfname,
			   rederrfname))
      return -1;
    char *cmd = mu_cat("cat ",resultfname);
    system(cmd);
    free(cmd);
  }

  free((void *) tmpdir);

  return 0;

}

