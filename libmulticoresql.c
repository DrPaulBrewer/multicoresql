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
#include "libmulticoresql.h"

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



void * xmalloc(size_t size){
  void * p = malloc(size);
  if (p==NULL){
    fprintf(stderr,"%s\n","Fatal error: out of memory\n");
    exit(1);
  }
  return p;
}

char * xcat(const char *s1, const char *s2){
  char * s = xmalloc(snprintf(NULL, 0, "%s%s", s1, s2) + 1);
  sprintf(s,"%s%s",s1,s2);
  return s;
}

char * xdup(const char *s){
  errno = 0;
  char *x = strdup(s);
  if ((x==NULL) || (errno!=0)){
    perror("out of memory");
    exit(1);
  }
  return x;
}

void abortOnError(const char *msg){
  if (errno!=0){
    perror(msg);
    exit(1);
  } 
}

FILE* xfopen(const char *fname, const char *mode){
  errno = 0;
  FILE *f = fopen(fname, mode);
  abortOnError(xcat("Fatal error on opening file ",fname));
  return f;
}

int xrmtmp(const char *dirname){
  if ((dirname) && (dirname==strstr(dirname, "/tmp/")))
    {
      char *fmt = "rm -rf %s";
      int len = snprintf(NULL, 0, fmt, dirname);
      char *cmd = xmalloc(len);
      sprintf(cmd, fmt, dirname);
      errno = 0;
      system(cmd);
      abortOnError(xcat("Fatal error removing directory ",dirname));
      free(cmd);
      return 0;
    }
  fprintf(stderr, "warning: attempt to rm -rf %s; not executed", dirname);
  return -1;
}

pid_t xrun(const char *bin, char * const* argv, const char *infile, const char *outfile){
  pid_t pid = fork();
  if (pid>0) return pid;
  if (pid==0){
    errno = 0;
    int rd, wd;
    if (infile) {
      rd = open(infile, O_RDONLY);
      if (errno!=0){
	perror(xcat("Fatal: xrun could not open input file ",infile));
	exit(EXIT_FAILURE);
      }
      dup2(rd,0);
      if (errno!=0){
	perror("Fatal: xrun could not set input file with dup2 ");
	exit(EXIT_FAILURE);
      }
    }
    if (outfile){
      wd = open(outfile, O_WRONLY | O_CREAT, 0700);
      if (errno!=0){
	perror(xcat("Fatal: xrun could not open output file ",outfile));
	exit(EXIT_FAILURE);
      }
      dup2(wd,1);
      if (errno!=0){
	perror("Fatal: xrun could not set output file with dup2 ");
	exit(EXIT_FAILURE);
      }
    }
    int err = execvp(bin, argv);
    if ((err!=0) || (errno!=0)){
      perror("Fatal: execvp failed ");
      exit(EXIT_FAILURE);
    }
  } 
  perror("Fatal: failed to fork and run command ");
  exit(EXIT_FAILURE);
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

int makeQueryCoreFile(struct CONF * conf, const char *fname, int getschema, int shardc, const char **shardv, const char *mapsql){

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
  
  FILE * qcfile = xfopen(fname,"w");

  fprintf(qcfile,
	  qTemplate,
	  conf->otablename);
  
  for(i=0;i<shardc;++i){
    if (shardv[i]){
      fprintf(qcfile, ".open %s\n", shardv[i]);
      fLoadExtensions(conf, qcfile);
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

int getcoreshardc(int i, int n, int c){
  int each = c/n;
  int extras = (i<(c%n))? 1: 0;
  return each+extras;
}

const char** getcoreshardv(int i, int n, int c, const char ** v){
  char *x=NULL;
  int shardc = getcoreshardc(i,n,c);
  const char **result = xmalloc(shardc*sizeof(x));
  int idx;
  for(idx=0; (idx*n+i)<c; ++idx){
    result[idx] = v[(idx*n+i)];
  }
  return result;
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

  char tmpdirt[] = "/tmp/mcoreXXXXXX";
  char *tmpdir = mkdtemp(tmpdirt);
  abortOnError("Fatal: unable to create temporary work directory");

  char reducefname[bufsize];
  FILE *reducef;

  char resultfname[bufsize];
  snprintf(resultfname, bufsize, "%s/result", tmpdir);
  FILE *resultf;

  if (reducesql){
    snprintf(reducefname, bufsize, "%s/query.reduce", tmpdir);
    reducef = xfopen(reducefname, "w");
    fLoadExtensions(conf, reducef);
    if (createtablesql)
      fprintf(reducef, "%s\n", createtablesql);
  }
  
  for(icore=0;icore<(conf->ncores);++icore){
    char fname[bufsize], resultname[bufsize];
    snprintf(fname, bufsize, "%s/query.sqlite.core.%.3d", tmpdir, icore);
    snprintf(resultname,bufsize,"%s/results.core.%.3d",tmpdir,icore);
    if (reducesql)
      fprintf(reducef, ".read %s \n", resultname);
    int getschema = (icore==0) && (createtablesql==NULL);
    int shardc = getcoreshardc(icore, conf->ncores, conf->shardc);
    const char **shardv = getcoreshardv(icore, conf->ncores, conf->shardc, conf->shardv);
    makeQueryCoreFile(conf, fname, getschema, shardc, shardv, mapsql);
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

