#include "libmulticoreutils.h"
#include "libmulticoresql1.h"

struct CONF * defaultconf(){
  typedef struct CONF conftype;
  struct CONF * c = xmalloc(sizeof(conftype));
  c->bin = getenv("MULTICORE_SQLITE3_BIN");
  if (c->bin==NULL)
    c->bin = "sqlite3";
  c->db = NULL;
  c->extensions = getenv("MULTICORE_SQLITE3_EXTENSIONS");
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

int opendb(struct CONF * conf, const char *dbdir){
  conf->isopen=0;
  if (conf==NULL){
    fprintf(stderr,"%s\n","Fatal: opendb received null CONF pointer");
    exit(EXIT_FAILURE);
  }
  if (dbdir==NULL){
    fprintf(stderr,"%s\n","Fatal: opendb received null dbdir pointer");
    exit(EXIT_FAILURE);
  }
  char *glob = xcat(dbdir,"/[0-9]*");
  wordexp_t p;
  int flags = WRDE_NOCMD; // do not run commands in shells like "$(evil)"
  int x = wordexp(glob, &p, flags);
  if (x==0){
    conf->shardc = p.we_wordc;
    // fprintf(stderr,"found %zd shards \n",conf->shardc);
    conf->shardv = (const char **) p.we_wordv;
    // mark conf as open if return values make sense
    if ((conf->shardc>0) &&
	(conf->shardv[0]) &&
	(conf->shardv[conf->shardc]==NULL)
	) conf->isopen=1;
  }
  free(glob);
  return (conf->isopen)? 0: -1;
}



int fLoadExtensions(struct CONF *conf, FILE *f){
  if ( (conf->extensions) && (strlen(conf->extensions)>0) ){
    char *ext = xdup(conf->extensions);
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

int makeQueryCoreFile(struct CONF * conf, const char *fname, const char *coredbname, int shardc, const char **shardv, const char *mapsql){

  int i;

  if ( (conf==NULL) || (fname==NULL) || (shardc==0) || (shardv==NULL) || (mapsql==NULL) || strlen(mapsql)==0 )
    return -1;

  FILE * qcfile = xfopen(fname,"w");

  int is_select = ( (mapsql[0]=='s') || (mapsql[0]=='S') );

  for(i=0;i<shardc;++i){
    if (shardv[i]){
      fprintf(qcfile, ".open %s\n", shardv[i]);
      fLoadExtensions(conf, qcfile);
      if (is_select){
	fprintf(qcfile, "attach database '%s' as 'resultdb';\n", coredbname); 
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

int query(struct CONF *conf,
	  const char *mapsql,
	  const char *createtablesql,
	  const char *reducesql)
{
  int icore;

  int status;

  size_t bufsize=255; // for constructing fnames
  
  pid_t worker[conf->ncores];

  errno=0;
  char tmpdirt[] = "/tmp/mcoreXXXXXX";
  char *tmpdir = mkdtemp(tmpdirt);
  abortOnError(xcat("Fatal: unable to create temporary work directory ", tmpdir));

  char reducefname[bufsize];
  FILE *reducef;

  char resultfname[bufsize];
  snprintf(resultfname, bufsize, "%s/result", tmpdir);
  FILE *resultf;

  if (reducesql){
    snprintf(reducefname, bufsize, "%s/query.reduce", tmpdir);
    reducef = xfopen(reducefname, "w");
    fprintf(reducef, ".open %s/coredb.000\n", tmpdir);
    fLoadExtensions(conf, reducef);
  }
  
  for(icore=0;icore<(conf->ncores);++icore){
    char fname[bufsize], resultname[bufsize], coredbname[bufsize];
    snprintf(fname, bufsize, "%s/query.sqlite.core.%.3d", tmpdir, icore);
    snprintf(resultname, bufsize, "%s/results.core.%.3d", tmpdir, icore);
    snprintf(coredbname,bufsize,"%s/coredb.%.3d",tmpdir,icore);
    if ((reducef) && (icore>0)){
      fprintf(reducef, "attach database '%s' as 'coredb%.3d';\n", coredbname, icore);
      fprintf(reducef, "insert into %s select * from coredb%.3d.%s;\n", conf->otablename, icore, conf->otablename);
      fprintf(reducef, "detach database 'coredb%.3d';\n", icore);
    }
    int shardc = getcoreshardc(icore, conf->ncores, conf->shardc);
    const char **shardv = getcoreshardv(icore, conf->ncores, conf->shardc, conf->shardv);
    makeQueryCoreFile(conf, fname, coredbname, shardc, shardv, mapsql);
    errno = 0;
    char * const argv[] = {"sqlite3", (char * const) NULL};
    worker[icore] = xrun(conf->bin,argv,fname,resultname);
    free(shardv);
  }

  // wait for workers
  for(icore=0;icore<conf->ncores;++icore)
    waitpid(worker[icore],&status,0);
  
  if (reducesql){
    fprintf(reducef, "%s\n", reducesql);
    fclose(reducef);
    char * const argv[] = {"sqlite3", (char *const) NULL};
    int reducer = xrun(conf->bin, argv, reducefname, resultfname);
    char *cmd = xcat("cat ",resultfname);
    waitpid(reducer, &status, 0);
    system(cmd);
    free(cmd);
  }

  return 0;

}

