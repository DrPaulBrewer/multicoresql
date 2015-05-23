/* libmulticoreutils.c Copyright 2015 Paul Brewer
 * Economic and Financial Technology Consulting LLC
 * All rights reserved.
 * common routines for all versions of multicoresql
 */

#include "libmulticoresql.h"

static void * mu_malloc(size_t size){
  void * p = malloc(size);
  if (p==NULL){
    fprintf(stderr,"%s\n","Fatal error: out of memory\n");
    exit(EXIT_FAILURE);
  }
  return p;
}

static char * mu_cat(const char *s1, const char *s2){
  char * s = mu_malloc(snprintf(NULL, 0, "%s%s", s1, s2) + 1);
  sprintf(s,"%s%s",s1,s2);
  return s;
}

static char * mu_dup(const char *s){
  errno = 0;
  char *x = strdup(s);
  if ((x==NULL) || (errno!=0)){
    perror("out of memory");
    exit(EXIT_FAILURE);
  }
  return x;
}

static void mu_abortOnError(const char *msg){
  if (errno!=0){
    perror(msg);
    exit(EXIT_FAILURE);
  } 
}

static FILE* mu_fopen(const char *fname, const char *mode){
  errno = 0;
  FILE *f = fopen(fname, mode);
  mu_abortOnError(mu_cat("Fatal error -- can not open file. The file may be inaccessible or missing,  File: ",fname));
  return f;
}

static int mu_getcoreshardc(int i, int n, int c){
  int each = c/n;
  int extras = (i<(c%n))? 1: 0;
  return each+extras;
}

static const char ** mu_getcoreshardv(int i, int n, int c, const char ** v){
  typedef char * pchar;
  int shardc = mu_getcoreshardc(i,n,c);
  const char ** result = mu_malloc(shardc*sizeof(pchar));
  int idx;
  for(idx=0; (idx*n+i)<c; ++idx){
    result[idx] = v[(idx*n+i)];
  }
  return result;
}

static int is_mu_temp(const char *fname){
  return ((fname) && (fname==strstr(fname,"/tmp/multicoresql-")));
}

static int is_mu_fatal(const char *msg){
  return ((msg) &&
	  ( (msg==strstr(msg,"fatal")) ||
	    (msg==strstr(msg,"Fatal")) ||
	    (msg==strstr(msg,"FATAL"))
	    ));
}

static int mu_remove_temp_dir(const char *dirname){
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

static int exists_mu_temp_file(const char *fname){
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

static int exists_mu_temp_done(const char *dirname){
  if (is_mu_temp(dirname)){
    size_t bufsize = 255;
    char fname[bufsize];
    snprintf(fname,bufsize,"%s/DONE",dirname);
    return exists_mu_temp_file(fname);
  }
  return 0;
}

static int mu_mark_temp_done(const char *dirname){
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

static const char * mu_create_temp_dir(){
  char * template = mu_dup("/tmp/multicoresql-XXXXXX");
  return (const char *) mkdtemp(template);
}

static int ok_mu_shard_name(const char *name){
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

static struct mu_SQLITE3_TASK * mu_define_task(const char *dirname, const char *taskname, int tasknum){
  typedef struct mu_SQLITE3_TASK mu_SQLITE3_TASK_type;
  struct mu_SQLITE3_TASK *task = mu_malloc(sizeof(mu_SQLITE3_TASK_type));
  task->pid=0;
  task->status=0;
  task->dirname = mu_dup(dirname);
  task->taskname = mu_dup(taskname);
  task->tasknum = tasknum;
  const char *fmt = "%s/%s.%s.%.3d";
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

static int mu_start_task(struct mu_SQLITE3_TASK *task, const char *abortmsg){
  errno = 0;
  const char *env_sqlite3_bin = getenv("MULTICORE_SQLITE3_BIN");
  const char *default_sqlite3_bin = "sqlite3";
  const char *bin =  (env_sqlite3_bin)? env_sqlite3_bin: default_sqlite3_bin;    
  char * const argv[] = { "sqlite3" , NULL };
  pid_t pid = fork();
  if (pid>0) {
    task->pid = pid;
    return 0;
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

static int mu_finish_task(struct mu_SQLITE3_TASK *task, const char *abortmsg){
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

static void mu_free_task(struct mu_SQLITE3_TASK *task){
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
  struct mu_SQLITE3_TASK *getshardnames_task =
    mu_define_task(tmpdir, "getshardnames", 0);
  FILE *getcmdf = mu_fopen(getshardnames_task->iname, "w");
  fprintf(getcmdf, shard_setup_fmt, tablename);
  fclose(getcmdf);
  int runstatus=0;
  mu_start_task(getshardnames_task, "Fatal Error in mu_create_shards_from_sqlite_table() while trying to start sqlite3 for a read-only operation. Check that sqlite3 is properly installed on your system and if sqlite3 is not on the PATH set environment variable MULTICORE_SQLITE3_BIN \n");
  runstatus = mu_finish_task(getshardnames_task, "Fatal Error in mu_create_shards_from_sqlite_table().  Errors occurred while reading the shardid column.  Check that the named database exists and the selected table has a shardid column. \n");
  char *cmdout = mu_read_small_file(getshardnames_task->oname);
  if (cmdout==NULL){
    fprintf(stderr, "Fatal Error in mu_create_shards_from_sqlite_table().  While reading the shardid column of the table, there were no usable values. All values read were either NULL or blank string. Before calling mu_create_shards_from_sqlite_table() make sure that the shardid column exists and has values to indicate a shardid for each row of the table.  \n");
    exit(EXIT_FAILURE);
  }
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
  struct mu_SQLITE3_TASK *makeshards_task =
    mu_define_task(tmpdir,"makeshards",0);
  FILE *makeshardsf = mu_fopen(makeshards_task->iname, "w");
  for(i=0;i<shardc;++i){
    if (ok_mu_shard_name(shardv[i]))
      fprintf(makeshardsf, shard_data_fmt, dbdir, shardv[i], tablename, tablename, shardv[i]);
    else
      fprintf(stderr,"Warning: mu_create_shards_from_sqlite_table() will ignore all data rows tagged with shardid %s .  Valid shard names may contain 0-9,a-z,A-Z,-,_, or . but may not begin with . \n", shardv[i]);
  }
  fclose(makeshardsf);
  mu_finish_task(makeshards_task, "Fatal Error detected by mu_create_shards_from_sqlite_table() while running sqlite3 to create the shard databases.  You should probably delete the shard databases and create the shards again after reviewing the messages below and fixing any correctable errors. \n");
  mu_free_task(getshardnames_task);
  mu_free_task(makeshards_task);
  free((void *) cmdout);
  free((void *) dbname2);
  mu_remove_temp_dir(tmpdir);
  free((void *) tmpdir);
  return 0;
}

static unsigned int mu_get_random_seed(void){
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
  struct mu_SQLITE3_TASK *createdb_task = mu_define_task(tmpdir, "createdb", 0);
  FILE *cmdf = mu_fopen(createdb_task->iname, "w");
  const char *cmdfmt = 
    ".open %s/%.3d\n"
    "pragma synchronous = 0;\n"
    ".read %s\n"
    ".import %s/%.3d %s\n";
    
  for(ishard=0;ishard<shardc;++ishard){
    fclose(csvshards[ishard]);
    fprintf(cmdf, cmdfmt, dbDir, ishard, schemaname, tmpdir, ishard, tablename);
  }
  fclose(cmdf);
  mu_start_task(createdb_task, "Fatal Error detected by mu_create_shards_from_csv() could not run sqlite3 to create shard databases.  No shard databases were created.  \n");
  mu_finish_task(createdb_task, "Fatal Error detected by mu_create_shards_from_csv().  An Error occurred while running sqlite3 to create the shard databases.  You should delete any newly generated shard databases and run again after fixing any correctable errors. \n");  
  mu_remove_temp_dir(tmpdir);
  mu_free_task(createdb_task);
  free((void *) tmpdir);
  return 0;
}

static const char *mu_sqlite3_extensions(void){
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

static int mu_fLoadExtensions(FILE *f){
  const char *exts = mu_sqlite3_extensions();
  errno = 0;
  if (exts)
    fputs(exts, f);
  return errno;
}


static int mu_makeQueryCoreFile3(struct mu_DBCONF * conf, const char *fname, int getschema, int shardc, const char **shardv, const char *mapsql){

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

int mu_query3(struct mu_DBCONF *conf,
	      const char *mapsql,
	      const char *createtablesql,
	      const char *reducesql)
{
  errno = 0;
  const char *tmpdir = mu_create_temp_dir();
  mu_abortOnError("Fatal error in mu_query3(): unable to create temporary work directory");

  struct mu_SQLITE3_TASK * mapsql_task[3];

  mapsql_task[0] = mu_define_task(tmpdir, "mapsql", 0);
  mapsql_task[1] = mu_define_task(tmpdir, "mapsql", 1);
  mapsql_task[2] = mu_define_task(tmpdir, "mapsql", 2);

  struct mu_SQLITE3_TASK * reducesql_task = NULL;
  FILE *reducef;

  if (reducesql){
    reducesql_task = mu_define_task(tmpdir, "reducesql", 0);
    reducef = mu_fopen(reducesql_task->iname, "w");
    mu_fLoadExtensions(reducef);
    if (createtablesql)
      fprintf(reducef, "%s\n", createtablesql);
    fprintf(reducef, "%s\n", "BEGIN TRANSACTION;");
  }
  
  int shardc;
  const char ** shardv;
  char * const argv[] = {"sqlite3", (char * const) NULL};
  char * enames[3];
  
  /* create block macro */
#define RUN_CORE(mycore) \
  if (reducesql) \
    fprintf(reducef, ".read %s \n", mapsql_task[mycore]->oname); \
  shardc = mu_getcoreshardc(mycore, conf->ncores, conf->shardc);		\
  shardv = (const char **) mu_getcoreshardv(mycore, conf->ncores, conf->shardc, conf->shardv); \
  mu_makeQueryCoreFile3(conf,  \
		       mapsql_task[mycore]->iname, \
		       (((mycore==0) && (reducesql!=NULL) && (createtablesql==NULL))? 1: 0), \
		       shardc,						\
		       shardv,						\
		       mapsql);						\
  errno = 0;								\
  mu_start_task(mapsql_task[mycore], "Fatal error detected by mu_query3() while attempting to run sqlite3 ");  \
  free(shardv);								\
  
  /* end of macro */

  /* expand macro and run 3 workers */
  RUN_CORE(0);
  RUN_CORE(1);
  RUN_CORE(2);

  // wait for workers to finish
  const char *abortmsg = "Fatal error detected by mu_query3() in mapsql task. \n";
  mu_finish_task(mapsql_task[0], abortmsg);
  mu_finish_task(mapsql_task[1], abortmsg);
  mu_finish_task(mapsql_task[2], abortmsg);
  
  if (reducesql){
    fprintf(reducef,"%s\n", "COMMIT;");
    fprintf(reducef, "%s\n", reducesql);
    fclose(reducef);
    mu_start_task(reducesql_task, "Fatal error detected by mu_query3() while attempting to run sqlite3");
    mu_finish_task(reducesql_task, "Fatal error detected by mu_query3() in reducesql task. ");
    char *cmd = mu_cat("cat ", reducesql_task->oname);
    system(cmd);
    free(cmd);
  }

  return 0;

}


struct mu_DBCONF * mu_opendb(const char *dbdir){
  typedef struct mu_DBCONF conftype;
  if (dbdir==NULL){
    fprintf(stderr,"%s\n","Fatal: mu_opendb received null dbdir pointer");
    exit(EXIT_FAILURE);
  }
  struct mu_DBCONF * c = mu_malloc(sizeof(conftype));
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
  c->isopen=0;
  char *glob = mu_cat(dbdir,"/[0-9]*");
  wordexp_t p;
  int flags = WRDE_NOCMD; // do not run commands in shells like "$(evil)"
  int x = wordexp(glob, &p, flags);
  if (x==0){
    c->shardc = p.we_wordc;
    // fprintf(stderr,"found %zd shards \n",c->shardc);
    c->shardv = (const char **) p.we_wordv;
    // mark c as open if return values make sense
    if ((c->shardc>1) &&
	(c->shardv[0]) &&
	(c->shardv[c->shardc]==NULL)
	) c->isopen=1;
  }
  free(glob);
  if (c->isopen==0){
    free(c);
    return NULL;
  } else
    return c;
}



static int mu_makeQueryCoreFile(struct mu_DBCONF * conf, const char *fname, const char *coredbname, int shardc, const char **shardv, const char *mapsql){

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

int mu_query(struct mu_DBCONF *conf,
	  const char *mapsql,
	  const char *createtablesql,
	  const char *reducesql)
{

  errno=0;
  const char *tmpdir = mu_create_temp_dir();
  mu_abortOnError(mu_cat("Fatal: unable to create temporary work directory ", tmpdir));

  int icore;

  struct mu_SQLITE3_TASK *mapsql_task[conf->ncores];
  for(icore=0;icore<conf->ncores;++icore)
    mapsql_task[icore] = mu_define_task(tmpdir, "mapsql", icore);
  
  struct mu_SQLITE3_TASK *reducesql_task = mu_define_task(tmpdir,"reducesql",0);
  
  FILE *reducef;
  
  if (reducesql){
    reducef = mu_fopen(reducesql_task->iname, "w");
    fprintf(reducef, ".open %s/coredb.000\n", tmpdir);
    fprintf(reducef, "%s;\n", "pragma synchronous = 0"); 
    mu_fLoadExtensions(reducef);
  }

  char * coredbname[conf->ncores];
  const size_t coredbnamesize = 255;

  const char *abortmsg_on_start = "Fatal error detected by mu_query() attempting to start sqlite3 ";
  const char *abortmsg_on_finish_map = "Fatal error detected by mu_query() in map task";
  const char *abortmsg_on_finish_reduce = "Fatal error detected by mu_query() in reduce task"; 
  
  for(icore=0;icore<(conf->ncores);++icore){
    coredbname[icore] = mu_malloc(coredbnamesize);
    snprintf(coredbname[icore],coredbnamesize,"%s/coredb.%.3d",tmpdir,icore);
    if ((reducef) && (icore>0)){
      fprintf(reducef, "attach database '%s' as 'coredb%.3d';\n", coredbname[icore], icore);
      fprintf(reducef, "insert into %s select * from coredb%.3d.%s;\n", conf->otablename, icore, conf->otablename);
      fprintf(reducef, "detach database 'coredb%.3d';\n", icore);
    }
    int shardc = mu_getcoreshardc(icore, conf->ncores, conf->shardc);
    const char **shardv = mu_getcoreshardv(icore, conf->ncores, conf->shardc, conf->shardv);
    mu_makeQueryCoreFile(conf,
			 mapsql_task[icore]->iname,
			 coredbname[icore],
			 shardc,
			 shardv,
			 mapsql);
    mu_start_task(mapsql_task[icore], abortmsg_on_start);
    free(shardv);
  }
  
  // wait for workers

  for(icore=0;icore<conf->ncores;++icore){
    mu_finish_task(mapsql_task[icore], abortmsg_on_finish_map);
  }

  if (reducesql){
    int reducer_status=0;
    fprintf(reducef, "%s\n", reducesql);
    fclose(reducef);
    mu_start_task(reducesql_task, abortmsg_on_start);
    mu_finish_task(reducesql_task, abortmsg_on_finish_reduce);
    char *cmd = mu_cat("cat ", reducesql_task->oname);
    system(cmd);
    free(cmd);
  }

  free((void *) tmpdir);
  mu_free_task(reducesql_task);
  for(icore=0;icore<conf->ncores;++icore){
    mu_free_task(mapsql_task[icore]);
    free( (void *) coredbname[icore]);
  }
  


  return 0;

}

