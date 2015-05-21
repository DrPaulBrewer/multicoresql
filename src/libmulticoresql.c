/* libmulticoreutils.c Copyright 2015 Paul Brewer
 * Economic and Financial Technology Consulting LLC
 * All rights reserved.
 * common routines for all versions of multicoresql
 */

#include "libmulticoresql.h"

void * mu_malloc(size_t size){
  void * p = malloc(size);
  if (p==NULL){
    fprintf(stderr,"%s\n","Fatal error: out of memory\n");
    exit(1);
  }
  return p;
}

char * mu_cat(const char *s1, const char *s2){
  char * s = mu_malloc(snprintf(NULL, 0, "%s%s", s1, s2) + 1);
  sprintf(s,"%s%s",s1,s2);
  return s;
}

char * mu_dup(const char *s){
  errno = 0;
  char *x = strdup(s);
  if ((x==NULL) || (errno!=0)){
    perror("out of memory");
    exit(EXIT_FAILURE);
  }
  return x;
}

void mu_abortOnError(const char *msg){
  if (errno!=0){
    perror(msg);
    exit(1);
  } 
}

FILE* mu_fopen(const char *fname, const char *mode){
  errno = 0;
  FILE *f = fopen(fname, mode);
  mu_abortOnError(mu_cat("Fatal error on opening file ",fname));
  return f;
}

pid_t mu_run(const char *bin, char * const* argv, const char *infile, const char *outfile, const char *errfile){
  errno = 0;
  pid_t pid = fork();
  if (pid>0) return pid;
  if (pid==0){
    errno = 0;
    int rd, wd;
    if (infile) {
      rd = open(infile, O_RDONLY);
      if (errno!=0){
	perror(mu_cat("Fatal: mu_run could not open input file ",infile));
	exit(EXIT_FAILURE);
      }
      dup2(rd,0);
      if (errno!=0){
	perror("Fatal: mu_run could not set input file with dup2 ");
	exit(EXIT_FAILURE);
      }
    }
    if (outfile){
      wd = open(outfile, O_WRONLY | O_CREAT, 0700);
      if (errno!=0){
	perror(mu_cat("Fatal: mu_run could not open output file ",outfile));
	exit(EXIT_FAILURE);
      }
      dup2(wd,1);
      if (errno!=0){
	perror("Fatal: mu_run could not set output file with dup2 ");
	exit(EXIT_FAILURE);
      }
    }
    if (errfile){
      wd = open(errfile, O_WRONLY | O_CREAT, 0700);
      if (errno!=0){
	perror(mu_cat("Fatal: mu_run could not open output file ",errfile));
	exit(EXIT_FAILURE);
      }
      dup2(wd,2);
      if (errno!=0){
	perror("Fatal: mu_run could not set error file with dup2 ");
	exit(EXIT_FAILURE);
      }      
    }
    int err = execvp(bin, argv);
    if ((err!=0) || (errno!=0)){
      fprintf(stderr,"Error while trying to run %s \n",bin);
      perror("Fatal: execvp failed ");
      exit(EXIT_FAILURE);
    }
  } 
  perror("Fatal: mu_run() failed to fork and run command ");
  exit(EXIT_FAILURE);
}

int mu_getcoreshardc(int i, int n, int c){
  int each = c/n;
  int extras = (i<(c%n))? 1: 0;
  return each+extras;
}

const char ** mu_getcoreshardv(int i, int n, int c, const char ** v){
  typedef char * pchar;
  int shardc = mu_getcoreshardc(i,n,c);
  const char ** result = mu_malloc(shardc*sizeof(pchar));
  int idx;
  for(idx=0; (idx*n+i)<c; ++idx){
    result[idx] = v[(idx*n+i)];
  }
  return result;
}

const char * mu_sqlite3_bin(){
  const char *envbin = getenv("MULTICORE_SQLITE3_BIN");
  const char *defbin = "sqlite3";
  return (envbin)? envbin: defbin;    
}

int is_mu_temp(const char *fname){
  return ((fname) && (fname==strstr(fname,"/tmp/multicoresql-")));
}

int is_mu_fatal(const char *msg){
  return ((msg) &&
	  ( (msg==strstr(msg,"fatal")) ||
	    (msg==strstr(msg,"Fatal")) ||
	    (msg==strstr(msg,"FATAL"))
	    ));
}

int mu_remove_temp_dir(const char *dirname){
  if (is_mu_temp(dirname))
    {
      char *fmt = "rm -rf %s";
      int len = 1+snprintf(NULL, 0, fmt, dirname);
      char *cmd = mu_malloc(len);
      snprintf(cmd, len, fmt, dirname);
      errno = 0;
      system(cmd);
      mu_abortOnError(mu_cat("Fatal error removing directory ",dirname));
      free(cmd);
      return 0;
    }
  fprintf(stderr, "warning:  mu_remove_temp_dir(): attempt to rm -rf %s; not executed", dirname);
  return -1;
}

int exists_mu_temp_file(const char *fname){
  FILE *f;
  int result;
  if (is_mu_temp(fname)){
    errno = 0;
    f = fopen(fname, "r");
    if (f==NULL)
      return 0;
    fclose(f);
    result = (errno==0);
    errno = 0;
    return result;
  }
  return 0;
}

int exists_mu_temp_done(const char *dirname){
  if (is_mu_temp(dirname)){
    size_t bufsize = 255;
    char fname[bufsize];
    snprintf(fname,bufsize,"%s/DONE",dirname);
    return exists_mu_temp_file(fname);
  }
  return 0;
}

int mu_mark_temp_done(const char *dirname){
  FILE *f;
  if (is_mu_temp(dirname)){
    size_t bufsize = 255;
    char fname[bufsize];
    snprintf(fname,bufsize,"%s/DONE",dirname);
    errno = 0;
    f = fopen(fname, "w");
    if (errno!=0){
      fprintf(stderr,"warning: mu_mark_temp_done(): could not create file %s",fname);
      perror(NULL);
      return -1;
    }
    fclose(f);
    if (errno!=0){
      fprintf(stderr,"warning: mu_mark_temp_done(): could not close file %s",fname);
      perror(NULL);
      return -1;
    }
    return 0;
  }
  fprintf(stderr,"warning: mu_mark_temp_done(): %s not recognized as a mu temp directory name. No action taken. \n", dirname);
  return -1;
}

const char * mu_create_temp_dir(){
  char * template = mu_dup("/tmp/multicoresql-XXXXXX");
  return (const char *) mkdtemp(template);
}

int ok_mu_shard_name(const char *name){
  int  i = 0;
  char c = name[0];
  if ( (c==0) || (c=='.') )
    return 0;
  while(c){
    if (! ( 
	   ((c>='0') && (c<='9')) ||
	   ((c>='a') && (c<='z')) ||
	   ((c>='A') && (c<='Z')) || 
	   (c=='.') || 
	   (c=='-') || 
	   (c=='_')
	    ))
      return 0;
    ++i;
    c = name[i];
  }
  return 1;
}

char * mu_read_small_file(const char *fname){
  errno = 0;
  struct stat fstats;

  if (fname==NULL)
    return NULL;
  
  if (stat(fname, &fstats)!=0)
    return NULL;
  
  if (errno!=0)
    return NULL;

  FILE *f = fopen(fname,"r");
  
  if (errno!=0)
    return NULL;

  size_t buflen = (size_t) fstats.st_size + 1;

  if (buflen<=1)
    return NULL;

  size_t buflimit = (size_t) (100*1024*1024);  // 100MB self-imposed limit

  if (buflen>buflimit){
    errno = 0;
    return NULL;
  }
  
  char *buf = (char *) mu_malloc(buflen);

  size_t rcount = fread(buf, 1, fstats.st_size, f);

  buf[rcount]=0;
  
  if (rcount < ((size_t) fstats.st_size))
    fprintf(stderr, "Warning: mu_read_small_file %s, read %zu bytes, expected %lld \n", fname, rcount, (unsigned long long) fstats.st_size);
  
  if (errno!=0){
    perror(NULL);
    return NULL;
  }

  return buf;

}

int mu_warn_run_status(const char *msg,
		       int status,
		       const char *bin,
		       const char *iname,
		       const char *oname,
		       const char *ename
		       ){
  if (status!=0){
    if (msg)
      fprintf(stderr,"%s :\n",msg);
    fprintf(stderr, "Bad exit status %d from:\n", status);
    if (bin)
      fputs(bin, stderr);
    if (iname)
      fprintf(stderr," < %s ",iname);
    if (oname)
      fprintf(stderr," > %s ",oname);
    if (ename)
      fprintf(stderr," 2> %s ",ename);
    fputs("\n",stderr);
  }
  if (ename){
    const char *errs = mu_read_small_file(ename);
    if (errs){
      fputs("The program ",stderr);
      if (bin)
	fputs(bin,stderr);
      fputs(" generated these errors:\n", stderr);
      fputs(errs, stderr);
      fputs("\n", stderr);
      free((void *) errs);
      return -1;
    }
  }
  return status;
}

struct mu_SQLITE3_TASK * mu_define_task(const char *dirname, const char *taskname, int tasknum){
  typedef struct mu_SQLITE3_TASK mu_SQLITE3_TASK_type;
  struct mu_SQLITE3_TASK *task = mu_malloc(sizeof(mu_SQLITE3_TASK_type));
  task->pid=0;
  task->status=0;
  task->dirname = mu_dup(dirname);
  task->taskname = mu_dup(taskname);
  task->tasknum = tasknum;
  const char *fmt = "%s/%s.%s.%3d";
  size_t bufsize = 1+snprintf(NULL,0,fmt,dirname,taskname,"progress", tasknum);
  char *iname = mu_malloc(bufsize);
  char *oname = mu_malloc(bufsize);
  char *ename = mu_malloc(bufsize);
  char *pname = mu_malloc(bufsize);
  snprintf(iname, bufsize, fmt, dirname, taskname, "in", tasknum);
  snprintf(oname, bufsize, fmt, dirname, taskname, "out", tasknum);
  snprintf(ename, bufsize, fmt, dirname, taskname, "err", tasknum);
  snprintf(pname, bufsize, fmt, dirname, taskname, "progress", tasknum);
  task->iname = (const char *) iname;
  task->oname = (const char *) oname;
  task->ename = (const char *) ename;
  task->pname = (const char *) pname;
  return task;
}

int mu_start_task(struct mu_SQLITE3_TASK *task, const char *abortmsg){
  errno = 0;
  const char *env_sqlite3_bin = getenv("MULTICORE_SQLITE3_BIN");
  const char *default_sqlite3_bin = "sqlite3";
  const char *bin =  (env_sqlite3_bin)? env_sqlite3_bin: default_sqlite3_bin;    
  char * const argv[] = { "sqlite3" , NULL };
  pid_t pid = fork();
  if (pid>0) {
    task->pid = pid;
    return pid;
  }
  if (pid==0){
    errno = 0;
    int rd, wd, ed;
    if (task->iname) {
      rd = open(task->iname, O_RDONLY);
      if (errno!=0){
	if (abortmsg){
	  fputs(abortmsg, stderr);
	  perror(mu_cat("Fatal: mu_run could not open input file ",task->iname));
	} 
	exit(EXIT_FAILURE);
      }
      dup2(rd,0);
      if (errno!=0){
	if (abortmsg){
	  fputs(abortmsg, stderr);
	  perror("Fatal: mu_run could not set input file with dup2 ");
	}
	exit(EXIT_FAILURE);
      }
    }
    if (task->oname){
      wd = open(task->oname, O_WRONLY | O_CREAT, 0700);
      if (errno!=0){
	if (abortmsg){
	  fputs(abortmsg, stderr);
	  perror(mu_cat("Fatal: mu_run could not open output file ",task->oname));
	}
	exit(EXIT_FAILURE);
      }
      dup2(wd,1);
      if (errno!=0){
	if (abortmsg){
	  fputs(abortmsg, stderr);
	  perror("Fatal: mu_run could not set output file with dup2 ");
	}
	exit(EXIT_FAILURE);
      }
    }
    if (task->ename){
      ed = open(task->ename, O_WRONLY | O_CREAT, 0700);
      if (errno!=0){
	if (abortmsg){
	  fputs(abortmsg, stderr);
	  perror(mu_cat("Fatal: mu_run could not open error file ",task->ename));
	}
	exit(EXIT_FAILURE);
      }
      dup2(ed,2);
      if (errno!=0){
	if (abortmsg){
	  fputs(abortmsg, stderr);
	  perror("Fatal: mu_run could not set error file with dup2 ");
	}
	exit(EXIT_FAILURE);
      }      
    }
    int err = execvp(bin, argv);
    if ((err!=0) || (errno!=0)){
      if (abortmsg){
      fputs(abortmsg, stderr);
      fprintf(stderr,"Error while trying to run %s \n",bin);
      perror("Fatal: execvp failed ");
      }
      exit(EXIT_FAILURE);
    } 
  }
  if (abortmsg){
    fputs(abortmsg, stderr);
    perror("Fatal: mu_run() failed to fork and run command ");
    exit(EXIT_FAILURE);
  }
  return errno;
}

int mu_finish_task(struct mu_SQLITE3_TASK *task, const char *abortmsg){
  waitpid(task->pid, &(task->status), 0);
  const char *errs = NULL;
  if (task->ename)
    errs = mu_read_small_file(task->ename);
  if ((task->status!=0) || (errs != NULL)){
    if (abortmsg){
      fprintf(stderr,"%s :\n",abortmsg);
      fprintf(stderr, "Exit status %d\n", task->status);
      fputs("sqlite3 task \n", stderr);
      if (task->iname)
	fprintf(stderr," input_file %s \n",task->iname);
      if (task->oname)
	fprintf(stderr," output_file %s \n",task->oname);
      if (task->ename)
	fprintf(stderr," error_file %s \n",task->ename);
      fputs("\n",stderr);
      if (task->ename){
	const char *errs = mu_read_small_file(task->ename);
	if (errs){
	  fputs("sqlite3 ",stderr);
	  fputs(" reported these errors:\n", stderr);
	  fputs(errs, stderr);
	  fputs("\n", stderr);
	  free((void *) errs);
	}
      }
      exit(EXIT_FAILURE);
    }
  }
  if ((task->status == 0) && (errs != NULL))
    return -1;
  return task->status;
}

void mu_free_task(struct mu_SQLITE3_TASK *task){
  free((void *) task->dirname);
  free((void *) task->taskname);
  free((void *) task->iname);
  free((void *) task->oname);
  free((void *) task->ename);
  free((void *) task->pname);
  free((void *) task);
}

int mu_create_shards_from_sqlite_table(const char *dbname, const char *tablename, const char *dbdir){
  typedef const char * pchar;
  const char *sqlite3_bin = mu_sqlite3_bin();
  const char *shard_setup_fmt = 
    "create temporary table shardids as select distinct shardid from %s where (shardid is not null) and (shardid!='');\n"
    "select count(*) from shardids;\n"
    "select shardid from shardids order by shardid;\n";   
  int i;
  int shardc;
  char **shardv;
  char * const dbname2 = mu_dup(dbname);
  char * const argv[] = {"sqlite3", dbname2, (char * const) NULL};
  const char *shard_data_fmt =
    "attach database '%s/%s' as 'shard';\n"
    "pragma synchronous = 0;\n"
    "create table shard.%s as select * from %s where shardid='%s';\n"
    "detach database shard;\n";
  const char *tmpdir = mu_create_temp_dir();
  const char *getshardnamesin  = mu_cat(tmpdir,"/getshardnames.in");
  const char *getshardnamesout = mu_cat(tmpdir,"/getshardnames.out");
  const char *getshardnameserr = mu_cat(tmpdir,"/getshardnames,err");
  FILE *getcmdf = mu_fopen(getshardnamesin, "w");
  fprintf(getcmdf, shard_setup_fmt, tablename);
  fclose(getcmdf);
  int runstatus=0;
  waitpid(mu_run(sqlite3_bin, argv, getshardnamesin, getshardnamesout, getshardnameserr), &runstatus, 0);
  if (mu_warn_run_status("Warning: mu_create_shards_from_sqlite_table()",
			 runstatus,
			 sqlite3_bin,
			 getshardnamesin,
			 getshardnamesout,
			 getshardnameserr))
    return -1;
  char *cmdout = mu_read_small_file(getshardnamesout);
  if (cmdout==NULL)
    return -1;
  char *tok0 = strtok(cmdout, "\n");
  if (tok0==NULL)
    return -1;
  shardc = (int) strtol(tok0, NULL, 10);
  if ((shardc<=0) || (errno!=0))
    return -1;
  shardv = mu_malloc((1+shardc)*sizeof(pchar));
  for(i=0;i<shardc;++i){
    shardv[i] = strtok(NULL, "\n");
  }
  shardv[shardc] = NULL;
  const char *makeshardsin  = mu_cat(tmpdir,"/makeshards.in");
  const char *makeshardsout = mu_cat(tmpdir,"/makeshards.out");
  const char *makeshardserr = mu_cat(tmpdir,"/makeshards.err");
  FILE *makeshardsf = mu_fopen(makeshardsin, "w");
  for(i=0;i<shardc;++i){
    if (ok_mu_shard_name(shardv[i]))
      fprintf(makeshardsf, shard_data_fmt, dbdir, shardv[i], tablename, tablename, shardv[i]);
  }
  fclose(makeshardsf);
  int shardstatus = 0;
  waitpid(mu_run(sqlite3_bin, argv, makeshardsin, makeshardsout, makeshardserr), &shardstatus, 0);
  shardstatus = mu_warn_run_status("Warning: mu_create_shards_from_sqlite_table() ",
				   shardstatus,
				   sqlite3_bin,
				   makeshardsin,
				   makeshardsout,
				   makeshardserr);
  free((void *) makeshardsin);
  free((void *) makeshardsout);
  free((void *) makeshardserr);
  free((void *) cmdout);
  free((void *) getshardnamesin);
  free((void *) getshardnamesout);
  free((void *) getshardnameserr);
  free((void *) dbname2);
  if (shardstatus==0)
    mu_remove_temp_dir(tmpdir);
  free((void *) tmpdir);
  return shardstatus;
}

unsigned int mu_get_random_seed(void){
  errno = 0;
  int fd = open("/dev/urandom",O_RDONLY);
  if (fd<0){
    perror("Warning: mu_get_random_seed failed to open /dev/urandom: "); 
    errno = 0;
    return 4570;
  }
  unsigned int result = 0;
  read(fd, (void *) &result, sizeof(result));
  close(fd);
  return result;
}

long int mu_get_long_int_or_die(const char *instring, const char *errfmt){
  errno = 0;
  long int result = strtol(instring, NULL, 10);
  if (errno!=0){
    fprintf(stderr, errfmt, instring);
    exit(EXIT_FAILURE);
  }
  return result;
}

int mu_create_shards_from_csv(const char *csvname, int skip, const char *schemaname, const char *tablename, const char *dbDir, int shardc){
  srand(mu_get_random_seed());
  const char *tmpdir = mu_create_temp_dir();
  errno = 0;
  if (mkdir(dbDir, 0700)){
    if (errno != EEXIST){
      fprintf(stderr,"Error: mu_create_shards_from_csv could not create requested directory %s\n", dbDir);
      perror(NULL);
      return -1;
    }
    errno=0;
  }
  FILE *csvf = mu_fopen(csvname,"r");
  size_t buf_size=32767;
  char csvbuf[buf_size];
  int linenum=0;
  FILE * csvshards[shardc];
  int ishard;
  for(ishard=0;ishard<shardc;++ishard){
    size_t name_size = 255;
    char shard_fname[name_size];
    snprintf(shard_fname, name_size, "%s/%.3d", tmpdir, ishard);
    csvshards[ishard] = mu_fopen(shard_fname,"w");
  }
  double dshardc = (double) shardc;
  while(fgets(csvbuf, buf_size, csvf)){
    linenum++;
    if ( linenum > skip ){
      double rand01 = ((double) rand())/((double) RAND_MAX);
      int fnum = (int) (dshardc*rand01);
      if (fnum==shardc)
	fnum=0;
      if ((fnum<0) || (fnum>=shardc)){
	fprintf(stderr,"Fatal: mu_create_shards_from_csv() generated random number %d not between 0 and %d \n", fnum, shardc);
	exit(EXIT_FAILURE);
      }
      fputs(csvbuf, csvshards[fnum]);
    }
  }
  fclose(csvf);
  const char *cmdfname = mu_cat(tmpdir,"/createdb");
  const char *cmderrname = mu_cat(tmpdir,"/createdb.err");
  FILE *cmdf = mu_fopen(cmdfname, "w");
  const char *cmdfmt = 
    ".open %s/%.3d\n"
    "pragma synchronous = 0;\n"
    ".read %s\n"
    ".import %s/%.3d %s\n";
    
  for(ishard=0;ishard<shardc;++ishard){
    fclose(csvshards[ishard]);
    fprintf(cmdf, cmdfmt, dbDir, ishard, schemaname, tmpdir, ishard, tablename);
    // fprintf(cmdf, ".open %s/%.3d\n", dbDir, ishard);
    // fprintf(cmdf, ".read %s\n", schemaname);
    // fprintf(cmdf, ".import %s/%.3d %s\n", tmpdir, ishard, tablename);
  }
  fclose(cmdf);
  const char *sqlite3_bin = mu_sqlite3_bin();
  char * const argv[] = {"sqlite3",NULL};
  pid_t pid = mu_run(sqlite3_bin, argv, cmdfname, NULL, cmderrname);
  int status;
  waitpid(pid, &status, 0);
  status = mu_warn_run_status("Warning: mu_create_shards_from_csv() ",
			      status,
			      sqlite3_bin,
			      cmdfname,
			      NULL,
			      cmderrname);
  if (status==0)
    mu_remove_temp_dir(tmpdir);
  free((void *) tmpdir);
  return status;
}

const char *mu_sqlite3_extensions(void){
  static char * exts = NULL;
  static int extdone = 0;
  if (extdone)
    return (const char *) exts;
  extdone = 1;
  const char *extensions = getenv("MULTICORE_SQLITE3_EXTENSIONS");
  if (extensions==NULL){
    return (const char *) exts;
  }
  size_t bufsize = 1024;
  exts = mu_malloc(bufsize);
  size_t offset = 0;
  char *e = mu_dup(extensions);
  char *tok = strtok(e, " ");
  errno = 0;
  while (tok && (offset<bufsize) && (!errno)){
    offset += snprintf(exts+offset,
		       bufsize-offset,
		       ".load %s\n",
		       tok);
    tok = strtok(NULL, " ");
  }
  free(e);
  return (const char *) exts;
}

int mu_fLoadExtensions(FILE *f){
  const char *exts = mu_sqlite3_extensions();
  errno = 0;
  if (exts)
    fputs(exts, f);
  return errno;
}


int mu_makeQueryCoreFile3(struct mu_CONF * conf, const char *fname, int getschema, int shardc, const char **shardv, const char *mapsql){

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
      mu_fLoadExtensions(qcfile);
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

int mu_query3(struct mu_CONF *conf,
	      const char *mapsql,
	      const char *createtablesql,
	      const char *reducesql)
{
  int icore;

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

  char rerrfname[bufsize];

  if (reducesql){
    snprintf(reducefname, bufsize, "%s/query.reduce", tmpdir);
    snprintf(rerrfname, bufsize, "%s/query.reduce.err", tmpdir);
    reducef = mu_fopen(reducefname, "w");
    mu_fLoadExtensions(reducef);
    if (createtablesql)
      fprintf(reducef, "%s\n", createtablesql);
    fprintf(reducef,"%s\n", "BEGIN TRANSACTION;");
  }
  
  char fname[bufsize], resultname[bufsize], errname[bufsize];
  int shardc;
  const char ** shardv;
  char * const argv[] = {"sqlite3", (char * const) NULL};
  char * enames[3];
  
  /* create block macro */
#define RUN_CORE(mycore) \
  icore = mycore; \
  snprintf(fname, bufsize, "%s/query.sqlite.core.%.3d", tmpdir, icore); \
  snprintf(resultname,bufsize,"%s/results.core.%.3d",tmpdir,icore); \
  snprintf(errname,bufsize,"%s/err.core.%.3d",tmpdir,icore);
  enames[icore] = errname;
  if (reducesql) \
    fprintf(reducef, ".read %s \n", resultname); \
  shardc = mu_getcoreshardc(icore, conf->ncores, conf->shardc); \
  shardv = (const char **) mu_getcoreshardv(icore, conf->ncores, conf->shardc, conf->shardv); \
  mu_makeQueryCoreFile3(conf,  \
		       fname,						\
		       (((icore==0) && (reducesql!=NULL) && (createtablesql==NULL))? 1: 0), \
		       shardc,						\
		       shardv,						\
		       mapsql);						\
  errno = 0;								\
  worker[icore] = mu_run(conf->bin,argv,fname,resultname,errname);	\
  free(shardv);								\
  
  /* end of macro */

  /* expand macro and run 3 workers */
  RUN_CORE(0);
  RUN_CORE(1);
  RUN_CORE(2);

  int status0 = 0;
  int status1 = 0;
  int status2 = 0;

  // wait for workers
  waitpid(worker[0], &status0, 0);
  waitpid(worker[1], &status1, 0);
  waitpid(worker[2], &status2, 0);
  
#define CHECK_CORE(mycore)						\
  if (mu_warn_run_status("mu_query worker",status0,conf->bin,NULL,NULL,enames[mycore])) \
    return -1; \

  CHECK_CORE(0);
  CHECK_CORE(1);
  CHECK_CORE(2);   
  
  if (reducesql){
    int reducer_status = 0;
    fprintf(reducef,"%s\n", "COMMIT;");
    fprintf(reducef, "%s\n", reducesql);
    fclose(reducef);
    char * const argv[] = {"sqlite3", (char *const) NULL};
    int reducer = mu_run(conf->bin, argv, reducefname, resultfname, rerrfname);
    waitpid(reducer, &reducer_status, 0);
    if (mu_warn_run_status("Warning in reduce query",reducer_status,conf->bin,reducefname,resultfname,rerrfname))
      return -1;
    char *cmd = mu_cat("cat ",resultfname);
    system(cmd);
    free(cmd);
  }

  return 0;

}


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

