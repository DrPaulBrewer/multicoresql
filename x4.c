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

struct CONF {
  const char *bin;
  const char *db;
  const char *extensions;
  const char *otablename;
  int isopen;
  int ncores;
  size_t shardc;
  const char **shardv;
};

void * xmalloc(size_t size){
  void * p = malloc(size);
  if (p==NULL){
    fprintf(stderr,"%s\n","Fatal error: out of memory\n");
    exit(1);
  }
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

int opendb(struct CONF * conf, const char *dbdir){
  conf->isopen=0;
  if (conf==NULL){
    fprintf(stderr,"%s\n","Fatal: opendb received null CONF pointer");
    exit(1);
  }
  if (dbdir==NULL){
    fprintf(stderr,"%s\n","Fatal: opendb received null dbdir pointer");
    exit(1);
  }
  char *glob = xcat(dbdir,"/[0-9]*");
  printf("%s\n",glob);
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
  return x;
}

int makeQueryCoreFile(struct CONF * conf, const char *fname, int getschema, int shardc, const char **shardv, const char *mapsql){

  int i;

  const char *qTemplate =
    ".mode insert %s\n" // output table name
    ;

  const char *qData =
    "%s ; \n";
  
  const char *qSchema =
    "create temporary table %s as %s ; \n"
    ".schema %s \n"
    "select * from %s; \n";
  
  FILE * qcfile = xfopen(fname,"w");

  fprintf(qcfile,
	  qTemplate,
	  conf->otablename);
  
  for(i=0;i<shardc;++i){
    if (shardv[i]){
      fprintf(qcfile, ".open %s \n", shardv[i]);
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

int makeQueryReduceFile(struct CONF *conf, const char *fname, const char *createtablesql, const char *reducesql){
  

}

int getshardc(int i, int n, int c){
  int each = c/n;
  int extras = (i<(c%n))? 1: 0;
  return each+extras;
}

const char** getshardv(int i, int n, int c, const char ** v){
  char *x=NULL;
  int shardc = getshardc(i,n,c);
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

  if (reducesql){
    snprintf(reducefname, bufsize, "%s/query.reduce", tmpdir);
    reducef = xfopen(reducefname, "w");
    fLoadExtensions(conf, reducef);
    if (createtablesql)
      fprintf(reducef, "%s ;\n", createtablesql);
  }
  
  for(icore=0;icore<(conf->ncores);++icore){
    char fname[bufsize], resultname[bufsize];
    snprintf(fname, bufsize, "%s/query.sqlite.core.%.3d", tmpdir, icore);
    snprintf(resultname,bufsize,"%s/results.core.%.3d",tmpdir,icore);
    if (reducesql)
      fprintf(reducef, ".read %s \n", resultname);
    int getschema = (icore==0) && (createtablesql==NULL);
    int shardc = getshardc(icore, conf->ncores, conf->shardc);
    const char **shardv = getshardv(icore, conf->ncores, conf->shardc, conf->shardv);
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
    fprintf(reducef, "%s ;\n", reducesql);
    fclose(reducef);
    char * const argv[] = {"sqlite3", (char *const) NULL};
    int reducer = xrun(conf->bin, argv, reducefname, NULL);
    waitpid(reducer, &status, 0);
  }

  return 0;

}

int main(int argc, char **argv){
  long int ncores = sysconf(_SC_NPROCESSORS_ONLN);      /* -c */
  char *dbname = NULL;  /* -d */
  char *tablename = NULL;  /* -t */
  char *mapsql = NULL; /* -m */
  char *reducesql = NULL; /* -r */
  int verbose = 0; /* -v */
  const char *default_sqlite3_bin = "sqlite3";
  

  struct CONF conf;

  conf.bin = getenv("MULTICORE_SQLITE3_BIN");
  if (conf.bin==NULL) 
    conf.bin = default_sqlite3_bin;
  
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

  opendb(&conf, dbname);
  return query(&conf, mapsql, NULL, reducesql);

}
