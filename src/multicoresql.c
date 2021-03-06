/* libmulticoreutils.c
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

#define _GNU_SOURCE
#include "multicoresql.h"

const size_t mu_error_len = 8191;
char mu_error_buf[8192];
size_t mu_error_cursor = 0;

const char *mu_error_oom =
  "Out of memory\n";
const char *mu_error_fname =
  "A serious file i/o error occurred. \nSome possibilities:  the file does not exist, permissions prevent access, the drive is full, the file is corrupted.\nFile name: %s \n";
const char *mu_error_fclose =
  "A serious file i/o error occurred while trying to save and close a file.\nThe file may be corrupted.\nFile name: %s\n";

const char *mu_error_string(){
  return ((mu_error_cursor)? mu_error_buf: NULL);
}

void mu_error_clear(){
  mu_error_cursor = 0;
}

#define MU_WARN(fmt, ...) do { 	      \
  int save_errno = errno;	      \
  if (mu_error_cursor < mu_error_len) \
    mu_error_cursor += snprintf(mu_error_buf+mu_error_cursor, mu_error_len-mu_error_cursor, fmt, ##__VA_ARGS__ ); \
  errno = save_errno;\
} while(0)

#define MU_WARN_OOM() MU_WARN("%s\n", mu_error_oom)

#define MU_WARN_FNAME(fname) MU_WARN(mu_error_fname, fname)

#define MU_WARN_IF_ERRNO() if (errno) MU_WARN("%s\n", strerror(errno))

#define MU_FPRINTF(fname, failval, f, fmt, ...) 	\
    if (fprintf(f, fmt, ##__VA_ARGS__ )<0){		\
      MU_WARN_FNAME(fname);				\
      MU_WARN("%s\n", strerror(errno));			\
      return failval;					\
    }


#define MU_FCLOSE_W(fname, failval, f)		\
  if (fclose(f)) {				\
    MU_WARN(mu_error_fclose, fname);		\
    MU_WARN_IF_ERRNO();				\
    return failval;				\
  }

#define MU_PRINTBUF(fmt, ...) \
  if (cursor<bufsize) \
    cursor+=snprintf(buf+cursor,bufsize-cursor,fmt,##__VA_ARGS__)

static char * mu_cat(const char *s1, const char *s2){
  char * s = malloc(snprintf(NULL, 0, "%s%s", s1, s2) + 1);
  if (NULL==s) {
    MU_WARN_OOM();
    return NULL;
  }
  sprintf(s,"%s%s",s1,s2);
  return s;
}

static FILE* mu_fopen(const char *fname, const char *mode){
  FILE *f = fopen(fname, mode);
  if (NULL==f){
    MU_WARN_FNAME(fname);
    MU_WARN_IF_ERRNO();
  }
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
  const char ** result = malloc(shardc*sizeof(pchar));
  if (NULL == result){
    MU_WARN_OOM();
    return NULL;
  }
  int idx;
  for(idx=0; (idx*n+i)<c; ++idx){
    result[idx] = v[(idx*n+i)];
  }
  return result;
}

static int is_mu_temp(const char *fname){
  return ((fname) && (fname==strstr(fname,"/tmp/multicoresql-")));
}

static int mu_remove_temp_dir(const char *dirname){
  if (is_mu_temp(dirname))
    {
      const char *fmt = "rm -rf %s";
      size_t bufsize = 255;
      char cmd[bufsize];
      size_t needed = snprintf(cmd, bufsize, fmt, dirname);
      if (needed>bufsize){
	MU_WARN("There was an unusual error while attempting to remove a directory\n The command name was too long.  \n Here is the partial command, which was not run:\n%s\n", cmd);
	return -1;
      }
      int rm_status = system(cmd);
      if (rm_status){
	MU_WARN("There was an error while attempting to remove this directory: %s\n", dirname);
	MU_WARN_IF_ERRNO();
	return -1;
      }
      return 0;
    }
  MU_WARN("mu_remove_temp_dir() received a request to delete a temporary directory, but the directory name was not in the right format.\n  The directory was NOT deleted.\n Directory name: %s\n", dirname);
  return -1;
}

static int exists_mu_temp_file(const char *fname){
  FILE *f;
  int result;
  if (is_mu_temp(fname)){
    f = fopen(fname, "r");
    if (f==NULL)
      return 0;
    fclose(f);
    return 1;
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
    size_t bufsize = 1024;
    char fname[bufsize];
    if (snprintf(fname,bufsize,"%s/DONE",dirname)>bufsize){
      MU_WARN("%s\n", "An unusual error occurred in mu_mark_temp_done().");
      return -1;
    }
    f = fopen(fname, "w");
    if (NULL==f){
      MU_WARN("mu_mark_temp_done() could not create file %s\n",fname);
      MU_WARN_IF_ERRNO();
      return -1;
    }
    MU_FCLOSE_W(fname, -1, f);
    return 0;
  }
  MU_WARN("mu_mark_temp_done(): %s not recognized as a mu temp directory name. No action taken. \n", dirname);
  return -1;
}

static const char * mu_create_temp_dir(){
  char * template = strdup("/tmp/multicoresql-XXXXXX");
  if (NULL==template){
    MU_WARN_OOM();
    return NULL;
  }
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
  struct stat fstats;

  if (NULL==fname)
    return NULL;

  if (stat(fname, &fstats)!=0)
    return NULL;

  FILE *f = fopen(fname,"r");

  if (NULL==f)
    return NULL;

  size_t buflen = (size_t) fstats.st_size + 1;

  if (buflen<=1)
    return NULL;

  size_t buflimit = (size_t) (100*1024*1024);  // 100MB self-imposed limit

  if (buflen>buflimit){
    return NULL;
  }

  char *buf = malloc(buflen);
  if (NULL==buf){
    MU_WARN_OOM();
    return NULL;
  }

  size_t rcount = fread(buf, 1, fstats.st_size, f);

  buf[rcount]=0;

  if (rcount < ((size_t) fstats.st_size)){
    MU_WARN("mu_read_small_file %s, read %zu bytes, expected %lld bytes, returning NULL for safety \n", fname, rcount, (unsigned long long) fstats.st_size);
    free(buf);
    return NULL;
  }

  return buf;

}

static const char *mu_dup_sql_or_read_file(const char *sql_or_fname){
  const int space = ' ';
  const int semicolon = ';';
  const char *result = NULL;
  if (NULL==sql_or_fname)
    return NULL;
  if ( (NULL==strchr(sql_or_fname,space)) &&
       (NULL==strchr(sql_or_fname,semicolon)) ){
    /* if there is no space or ; the argument is not valid sql
     * but might be a filename
     */
    result = mu_read_small_file(sql_or_fname);
    if (NULL==result){
      MU_WARN_FNAME(sql_or_fname);
      MU_WARN_IF_ERRNO();
      return NULL;
    }
    return result;
  }
  /* mu_read_small_file uses malloc() so */
  /* we use strdup here to allow free() to be safe */
  result = strdup(sql_or_fname);
  if (NULL==result){
    MU_WARN_OOM();
    return NULL;
  }
  return result;
}

static struct mu_SQLITE3_TASK * mu_define_task(const char *dirname, const char *mydbname, const char *taskname, int tasknum){
  if ((NULL==dirname) || (NULL==taskname) || (tasknum<0) ){
    MU_WARN("%s\n", "mu_define_task detected one or more invalid or NULL parameters");
    return NULL;
  }
  typedef struct mu_SQLITE3_TASK mu_SQLITE3_TASK_type;
  struct mu_SQLITE3_TASK *task = malloc(sizeof(mu_SQLITE3_TASK_type));
  if (NULL==task){
    MU_WARN_OOM();
    return NULL;
  }
  task->pid=0;
  task->status=0;
  task->dirname = strdup(dirname);
  task->taskname = strdup(taskname);
  if ((NULL==task->dirname) || (NULL==task->taskname)){
    MU_WARN_OOM();
    return NULL;
  }
  task->tasknum = tasknum;
  const char *fmt = "%s/%s.%s.%.3d";
  size_t bufsize = 1+snprintf(NULL,0,fmt,dirname,taskname,"progress", tasknum);
  char *iname  = malloc(bufsize);
  char *oname  = malloc(bufsize);
  char *ename  = malloc(bufsize);
  char *pname  = malloc(bufsize);
  char *dbname = (mydbname)? strdup(mydbname): malloc(bufsize);
  if ((NULL==iname) || (NULL==oname) || (NULL==ename) || (NULL==pname)){
    MU_WARN_OOM();
    return NULL;
  }
  int ni = snprintf(iname, bufsize, fmt, dirname, taskname, "in", tasknum);
  int no = snprintf(oname, bufsize, fmt, dirname, taskname, "out", tasknum);
  int ne = snprintf(ename, bufsize, fmt, dirname, taskname, "err", tasknum);
  int np = snprintf(pname, bufsize, fmt, dirname, taskname, "progress", tasknum);
  int nd = (mydbname)? 1: snprintf(dbname,bufsize, fmt, dirname, taskname, "db", tasknum);
  if ((ni>bufsize) || (no>bufsize) || (ne>bufsize) || (np>bufsize) || (nd>bufsize)){
    MU_WARN("mu_define_task() An unusual error occurred while setting up task %s\n", taskname);
    return NULL;
  }
  task->iname = (const char *) iname;
  task->oname = (const char *) oname;
  task->ename = (const char *) ename;
  task->pname = (const char *) pname;
  task->dbname = (const char *) dbname;
  return task;
}

static int mu_start_task(struct mu_SQLITE3_TASK *task, const char *errormsg){
  const char *env_sqlite3_bin = getenv("MULTICORE_SQLITE3_BIN");
  const char *default_sqlite3_bin = "sqlite3";
  const char *bin =  (env_sqlite3_bin)? env_sqlite3_bin: default_sqlite3_bin;
  char * const argv[] = { "sqlite3" , (char * const) task->dbname, NULL };
  pid_t pid = fork();
  if (pid>0) {
    task->pid = pid;
    return 0;
  }
  if (pid==0){
    int rd, wd, ed;
    if (task->ename){
      ed = open(task->ename, O_WRONLY | O_CREAT, 0700);
      if (ed<0){
	if (errormsg){
	  fputs(errormsg, stderr);
	  perror(mu_cat("Fatal: mu_start_task could not open error file ",task->ename));
	}
	exit(EXIT_FAILURE);
      }
      if (dup2(ed,2)<0){
	if (errormsg){
	  fputs(errormsg, stderr);
	  perror("Fatal: mu_start_task could not set error file with dup2 ");
	}
	exit(EXIT_FAILURE);
      }

    }
    if (task->iname) {
      rd = open(task->iname, O_RDONLY);
      if (rd<0){
	if (errormsg){
	  fputs(errormsg, stderr);
	  perror(mu_cat("Fatal: mu_run could not open input file ",task->iname));
	}
	exit(EXIT_FAILURE);
      }
      if (dup2(rd,0)<0){
	if (errormsg){
	  fputs(errormsg, stderr);
	  perror("Fatal: mu_start_task could not set input file with dup2 ");
	}
	exit(EXIT_FAILURE);
      }
    }
    if (task->oname){
      wd = open(task->oname, O_WRONLY | O_CREAT, 0700);
      if (wd<0){
	if (errormsg){
	  fputs(errormsg, stderr);
	  perror(mu_cat("Fatal: mu_start_task could not open output file ",task->oname));
	}
	exit(EXIT_FAILURE);
      }
      if (dup2(wd,1)<0){
	if (errormsg){
	  fputs(errormsg, stderr);
	  perror("Fatal: mu_start_task could not set output file with dup2 ");
	}
	exit(EXIT_FAILURE);
      }
    }
    int err = execvp(bin, argv);
    if (err){
      if (errormsg){
	fputs(errormsg, stderr);
	fprintf(stderr,"Error while trying to run %s \n",bin);
	perror("Fatal: execvp failed ");
      }
      exit(EXIT_FAILURE);
    }
    fprintf(stderr,"An unusual error occurred with execvp() while trying to run %s \n",bin);
    exit(EXIT_FAILURE);
  }
  if (errormsg){
    MU_WARN("%s\n", errormsg);
  }
  MU_WARN("mu_start_task() failed to fork and run %s\n", bin);
  MU_WARN_IF_ERRNO();
  return -1;
}

static int mu_finish_task(struct mu_SQLITE3_TASK *task, const char *errormsg){
  if (NULL==task){
    MU_WARN("%s\n", "An unusual error occurred. NULL task pointer in mu_finish_task()");
    return -1;
  }
  waitpid(task->pid, &(task->status), 0);
  const char *errs = NULL;
  if (task->ename)
    errs = mu_read_small_file(task->ename);
  if ((task->status!=0) || (errs)){
    if (errormsg)
      MU_WARN("%s\n", errormsg);
    MU_WARN("Exit status %d\n", task->status);
    MU_WARN("sqlite3 task %s %.3d\n", task->taskname, task->tasknum);
    if (task->dbname)
      MU_WARN(" dbfile %s \n", task->dbname);
    if (task->iname)
      MU_WARN(" input_file %s \n",task->iname);
    if (task->oname)
      MU_WARN(" output_file %s \n",task->oname);
    if (task->ename)
      MU_WARN(" error_file %s \n",task->ename);
    if (task->ename){
      const char *errs = mu_read_small_file(task->ename);
      if (errs){
	MU_WARN("sqlite3 reported these errors:\n", stderr);
	MU_WARN("%s\n", errs);
	free((void *) errs);
      }
    }
    return -1;
  }
  return 0;
}

static void mu_free_task(struct mu_SQLITE3_TASK *task){
  if (task){
    free((void *) task->dirname);
    free((void *) task->taskname);
    free((void *) task->iname);
    free((void *) task->oname);
    free((void *) task->ename);
    free((void *) task->pname);
    free((void *) task->dbname);
    free((void *) task);
  }
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
  const char *shard_data_each_fmt =
    "attach database '%s/%s' as 'shard';\n"
    "create table shard.%s as select * from %s where shardid='%s';\n"
    "detach database shard;\n";
  const char *tmpdir = mu_create_temp_dir();
  if (NULL==tmpdir)
    return -1;
  struct mu_SQLITE3_TASK *getshardnames_task =
    mu_define_task(tmpdir, dbname, "getshardnames", 0);
  if (NULL==getshardnames_task)
    return -1;
  FILE *getcmdf = mu_fopen(getshardnames_task->iname, "w");
  if (NULL==getcmdf)
    return -1;
  MU_FPRINTF(getshardnames_task->iname, -1, getcmdf, shard_setup_fmt, dbname, tablename);
  MU_FCLOSE_W(getshardnames_task->iname, -1, getcmdf);
  int runstatus=0;
  if (mu_start_task(getshardnames_task, "Fatal Error in mu_create_shards_from_sqlite_table() while trying to start sqlite3 for a read-only operation. Check that sqlite3 is properly installed on your system and if sqlite3 is not on the PATH set environment variable MULTICORE_SQLITE3_BIN \n"))
    return -1;
  runstatus = mu_finish_task(getshardnames_task, "Fatal Error in mu_create_shards_from_sqlite_table().  Errors occurred while reading the shardid column.  Check that the named database exists and the selected table has a shardid column. \n");
  if (runstatus)
    return -1;
  char *cmdout = mu_read_small_file(getshardnames_task->oname);
  if (cmdout==NULL){
    MU_WARN("%s\n", "Fatal Error in mu_create_shards_from_sqlite_table().  While reading the shardid column of the table, there were no usable values. All values read were either NULL or blank string. Before calling mu_create_shards_from_sqlite_table() make sure that the shardid column exists and has values to indicate a shardid for each row of the table.");
    return -1;
  }
  char *tok0 = strtok(cmdout, "\n");
  if (tok0==NULL){
    MU_WARN("%s\n", "An unusual error occurred in mu_create_shards_from_sqlite_table().  strtok() was unable to parse the data returned from examining the shardid column of the database table.");
    return -1;
  }
  shardc = (int) strtol(tok0, NULL, 10);
  if (shardc<=0){
    MU_WARN("An unusual error occurred in mu_create_shards_from_sqlite_table() while parsing the data returned from examining the shardid column of the database table. On the first line of data I expected to get the number of shards, but instead received: %s\n", tok0);
    return -1;
  }
  shardv = malloc((1+shardc)*sizeof(pchar));
  if (NULL==shardv){
    MU_WARN_OOM();
    return -1;
  }
  for(i=0;i<shardc;++i){
    shardv[i] = strtok(NULL, "\n");
  }
  shardv[shardc] = NULL;
  struct mu_SQLITE3_TASK *makeshards_task =
    mu_define_task(tmpdir, dbname, "makeshards",0);
  if (NULL==makeshards_task)
    return -1;
  FILE *makeshardsf = mu_fopen(makeshards_task->iname, "w");
  if (NULL==makeshardsf)
    return -1;
  for(i=0;i<shardc;++i){
    if (ok_mu_shard_name(shardv[i])){
      MU_FPRINTF(makeshards_task->iname, -1, makeshardsf, shard_data_each_fmt, dbdir, shardv[i], tablename, tablename, shardv[i]);
    } else {
      MU_WARN("Warning: mu_create_shards_from_sqlite_table() will ignore all data rows tagged with shardid %s .  Valid shard names may contain 0-9,a-z,A-Z,-,_, or . but may not begin with . \n", shardv[i]);
    }
  }
  MU_FCLOSE_W(makeshards_task->iname, -1, makeshardsf);
  if (mu_start_task(makeshards_task, "mu_create_shards_from_sqlite_table() was unable to begin the creating the shard databases."))
    return -1;
  if (mu_finish_task(makeshards_task, "Fatal Error detected by mu_create_shards_from_sqlite_table() while running sqlite3 to create the shard databases.  You should probably delete the newly created shard databases and run this creation task again after reviewing the messages below and fixing any correctable errors. \n"))
    return -1;
  mu_free_task(getshardnames_task);
  mu_free_task(makeshards_task);
  free((void *) cmdout);
  mu_remove_temp_dir(tmpdir);
  free((void *) tmpdir);
  return 0;
}

static unsigned int mu_get_random_seed(void){
  /* We would rather initialize from /dev/urandom */
  /* But will use remainder of unix time by 7919 in case urandom does not work */
  unsigned int backuprv = (unsigned int) (time(NULL) % 7919);
  int fd = open("/dev/urandom",O_RDONLY);
  if (fd<0){
    return backuprv;
  }
  unsigned int result = 0;
  int count = read(fd, (void *) &result, sizeof(result));
  if (count<=0){
    return backuprv;
  }
  close(fd);
  return result;
}

int mu_create_shards_from_csv(const char *csvname, int skip, const char *schema, const char *tablename, const char *dbDir, int shardc){
  /* inquire as to the maximum number of permissible open files */
  /* if we get back a number that is greater than 20, take it seriously. */
  /* and check the sharding request against this system limit */
  long int max_open_files = sysconf(_SC_OPEN_MAX);
  if (max_open_files>20){
    max_open_files = max_open_files-10;
    if (((long int) shardc) > max_open_files){
      MU_WARN("mu_create_shards_from_csv received a request to create %d shards but the maximum number we can create is limited by the maximum number of simultaneously open files to approximately %ld \n", shardc, max_open_files);
      return -1;
    }
  }


  const char *createsql = mu_dup_sql_or_read_file(schema);

  const char *no_create_error =
    "mu_create_shards_from_csv():  Someone (maybe you) omitted the 'create table' statement needed to create the tables from the csv files.  No further processing is possible.  No shard databases were created.";

  if (NULL==createsql){
    MU_WARN("%s\n", no_create_error);
    return -1;
  }

  if ( (NULL==strcasestr(createsql, "create table"))
       && (NULL==strcasestr(createsql, ".read "))){
    MU_WARN("%s\n", no_create_error);
    MU_WARN("mu_create_shards_from_csv() received this instead:\n%s\n", createsql);
    return -1;
  }

  srand(mu_get_random_seed());

  const char *tmpdir = mu_create_temp_dir();
  if (NULL==tmpdir)
    return -1;
  if (mkdir(dbDir, 0700)){
    if (errno != EEXIST){
      MU_WARN("mu_create_shards_from_csv could not create requested directory %s\n", dbDir);
      MU_WARN_IF_ERRNO();
      return -1;
    }
  }
  FILE *csvf = mu_fopen(csvname,"r");
  if (NULL==csvf)
    return -1;
  size_t buf_size=128*1024;
  char csvbuf[buf_size];
  long int linenum=0;
  FILE * csvshards[shardc];
  int ishard;
  for(ishard=0;ishard<shardc;++ishard){
    size_t name_size = 255;
    char shard_fname[name_size];
    if (snprintf(shard_fname, name_size, "%s/%.3d", tmpdir, ishard)>name_size){
      MU_WARN("%s\n", "An unusual error occurred while setting up shard file names in mu_create_shards_from_csv()");
      return -1;
    }
    csvshards[ishard] = mu_fopen(shard_fname,"w");
    if (NULL==csvshards[ishard])
      return -1;
  }
  double dshardc = (double) shardc;
  while(linenum < (long int) skip){
    linenum++;
    fgets(csvbuf, buf_size, csvf);
  }
  while(fgets(csvbuf, buf_size, csvf)){
    linenum++;
    double rand01 = ((double) rand())/((double) RAND_MAX);
    int fnum = (int) (dshardc*rand01);
    if (fnum==shardc)
      fnum=0;
    if ((fnum<0) || (fnum>=shardc)){
      MU_WARN("An unusual error occurred while copying the input csv file into multiple temporary csv files.  mu_create_shards_from_csv() generated random number %d not between 0 and %d \n", fnum, shardc);
      return -1;
    }
    int written = fputs(csvbuf, csvshards[fnum]);
    if (written<0){
      MU_WARN("%s\n", "An error occurred in mu_create_shards_from_csv() while writing data from the input csv file into a temporary csv file.");
      MU_WARN_IF_ERRNO();
      return -1;
    }
  }
  fclose(csvf);
  struct mu_SQLITE3_TASK *createdb_task = mu_define_task(tmpdir, NULL, "createdb", 0);
  if (NULL==createdb_task)
    return -1;
  FILE *cmdf = mu_fopen(createdb_task->iname, "w");
  if (NULL==cmdf)
    return -1;
  const char *cmdfmt =
    ".open %s/%.3d\n"
    "%s\n"
    ".import %s/%.3d %s\n";

  for(ishard=0;ishard<shardc;++ishard){
    if (fclose(csvshards[ishard])){
      MU_WARN("A serious file I/O error occurred writing shard file number %d\n", ishard);
      MU_WARN_IF_ERRNO();
      MU_WARN("This file is probably corrupt. The %s directory containing the csv shards may be deleted.\n", tmpdir);
      return -1;
    }
    int written = fprintf(cmdf, cmdfmt, dbDir, ishard, createsql, tmpdir, ishard, tablename);
    if (written<0){
      MU_WARN("%s\n", "mu_create_shards_from_csv() detected an error while writing a command file to create the shard databases. ");
      MU_WARN_IF_ERRNO();
      free((void *) createsql);
      return -1;
    }
  }

  free((void *) createsql);

  MU_FCLOSE_W(createdb_task->iname, -1, cmdf);
  if (mu_start_task(createdb_task, "Fatal Error detected by mu_create_shards_from_csv() could not run sqlite3 to create shard databases.  No shard databases were created.  \n"))
    return -1;
  if (mu_finish_task(createdb_task, "Fatal Error detected by mu_create_shards_from_csv().  An Error occurred while running sqlite3 to create the shard databases.  You should delete any newly generated shard databases and run again after fixing any correctable errors. \n"))
    return -1;
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
  if (NULL==extensions){
    return NULL;
  }
  size_t bufsize = 1024;
  exts = malloc(bufsize);
  if (NULL==exts){
    MU_WARN_OOM();
    return NULL;
  }
  size_t offset = 0;
  char *e = strdup(extensions);
  if (NULL==e){
    MU_WARN_OOM();
    return NULL;
  }
  char *tok = strtok(e, " ");
  while (tok && (offset<bufsize)){
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
  int written = 0;
  if (exts)
    written = fputs(exts, f);
  return (written<0)? -1: 0;
}



struct mu_QUERY * mu_create_query(const char *mapsql_or_fname,
				const char *createtablesql_or_fname,
				const char *reducesql_or_fname)
{

  const char *mu_error_omit_mapsql =
    "Error:  Someone (maybe you) omitted the 'mapsql' sql statements to be executed on each of the database shards. \nNo further processing is possible. \nThe query will not run.";

  const char *mu_error_badsql =
    "Error:  Could not read the sqlite statements or commands. \nThe query will not run.";

  typedef struct mu_QUERY muquery;

  if (NULL==mapsql_or_fname){
    MU_WARN("%s\n", mu_error_omit_mapsql);
    return NULL;
  }

  const char * mapsql = mu_dup_sql_or_read_file(mapsql_or_fname);

  if (NULL==mapsql){
    MU_WARN("%s\n", mu_error_omit_mapsql);
    return NULL;
  }

  const char * createtablesql = NULL;

  if (createtablesql_or_fname){
    createtablesql = mu_dup_sql_or_read_file(createtablesql_or_fname);
    if (NULL == createtablesql){
      MU_WARN("%s\n", mu_error_badsql);
      free( (void *) mapsql);
      return NULL;
    }
  }

  const char * reducesql = NULL;

  if (reducesql_or_fname){
    reducesql = mu_dup_sql_or_read_file(reducesql_or_fname);
    if (NULL == reducesql){
      MU_WARN("%s\n", mu_error_badsql);
      free( (void *) mapsql);
      if (createtablesql)
	free( (void *) createtablesql);
      return NULL;
    }
  }

  struct mu_QUERY *q = malloc(sizeof(muquery));
  if (NULL==q){
    MU_WARN_OOM();
    if (mapsql)
      free((void *) mapsql);
    if (reducesql)
      free((void *) reducesql);
    if (createtablesql)
      free((void *) createtablesql);
    return NULL;
  }

  q->mapsql = mapsql;
  q->reducesql = reducesql;
  q->createtablesql = createtablesql;

  return q;

}

const char *mu_error_null_dbconf =
  "Error:  Can not determine a database directory for this query. \nReceived a null pointer instead of a pointer to a database configuration. \nThe query will not run. \n";

const char *mu_error_null_query =
  "Error: Did not receive a query to execute.\n";



struct mu_DBCONF * mu_opendb(const char *dbdir){
  typedef struct mu_DBCONF conftype;
  if (NULL==dbdir){
    MU_WARN("%s\n","Fatal: mu_opendb received a NULL instead of the /path/to/shards directory ");
    return NULL;
  }
  struct mu_DBCONF * c = malloc(sizeof(conftype));
  if (NULL==c)
    return NULL;
  c->db = NULL;
  c->otablename = "maptable";
  c->isopen = 0;
  c->ncores = (int) sysconf(_SC_NPROCESSORS_ONLN);
  if ((c->ncores<=0) || (c->ncores>255)){
    /* number of cores not detected properly by sysconf.  This should not be fatal */
    /* So we specify 2 cores in this situation and a developer can change it in the returned mu_DBCONF if they know better. */
    c->ncores = 2;
  }
  c->shardc = 0;
  c->shardv = NULL;
  c->isopen=0;
  char *glob = mu_cat(dbdir,"/*");
  if (NULL==glob)
    return NULL;
  wordexp_t p;
  int flags = WRDE_NOCMD; // do not run commands in shells like "$(evil)"
  int w = wordexp(glob, &p, flags);
  if (w || (p.we_wordc<2) ){
    MU_WARN("mu_opendb() failed to find any shard database files in %s\n", glob);
    free(c);
    return NULL;
  }
  c->shardc = p.we_wordc;
  // fprintf(stderr,"found %zd shards \n",c->shardc);
  c->shardv = (const char **) p.we_wordv;
  // mark c as open if return values make sense
  if ((c->shardc>1) &&
      (c->shardv[0]) &&
      (NULL==c->shardv[c->shardc])
      ){
        c->isopen=1;
        if ((c->ncores)>(c->shardc)) c->ncores = c->shardc;  // can use at most shardc cores -- pragmatic fix for issue #1
  }
  free(glob);
  if (c->isopen==0){
    free(c);
    MU_WARN("mu_opendb() failed to open the database directory %s.\nCheck that the directory is non-empty at contains at least 2 files. \n", dbdir);
    return NULL;
  }
  return c;
}

static int is_mu_select(const char *sqlstr){
  size_t i=0;
  const char space = ' ';
  const char tab = '\t';
  /* If the string is blank or all spaces/tabs the null-terminator will end this while loop */
  while ( (space==sqlstr[i]) || (tab==sqlstr[i]) )
    ++i;
  return (('s'==sqlstr[i]) || ('S'==sqlstr[i]));
}

static int mu_makeQueryCoreFile(struct mu_DBCONF * conf, const char *fname, const char *coredbname, int shardc, const char **shardv, const char *mapsql){

  int i;

  if (NULL==conf) MU_WARN("%s\n","mu_makeQueryCoreFile detected conf==NULL");
  if (NULL==fname) MU_WARN("%s\n","mu_makeQueryCoreFile detected fname==NULL");
  if (0==shardc) MU_WARN("%s\n", "mu_makeQueryCoreFile detected shardc==0");
  if (NULL==shardv) MU_WARN("%s\n","mu_makeQueryCoreFile detected shardv==NULL");
  if (NULL==mapsql) MU_WARN("%s\n","mu_makeQueryCoreFile detected mapsql==NULL");

  if ( (conf==NULL) || (fname==NULL) || (shardc==0) || (shardv==NULL) || (mapsql==NULL) || strlen(mapsql)==0 ){
    MU_WARN("%s\n", "mu_makeQueryCoreFile detected one or more invalid or NULL required parameters");
    return -1;
  }

  size_t bufsize = (1024+strlen(mapsql))*shardc+1024;
  size_t cursor = 0;
  char *buf = malloc(bufsize);
  if (NULL==buf){
    MU_WARN_OOM();
    return -1;
  }


  int is_select = is_mu_select(mapsql);

  const char *exts = mu_sqlite3_extensions();

  for(i=0;i<shardc;++i){
    if (shardv[i]){
      MU_PRINTBUF(".open %s\n", shardv[i]);
      MU_PRINTBUF("%s\n",".bail on");
      if (exts)
	MU_PRINTBUF("%s\n", exts);
      if (is_select){
	MU_PRINTBUF("attach database '%s' as 'resultdb';\n", coredbname);
	if (i==0){
	  MU_PRINTBUF("create table resultdb.%s as %s\n",
		  conf->otablename,
		  mapsql);
	} else {
	  MU_PRINTBUF("insert into resultdb.%s %s\n",
		  conf->otablename,
		  mapsql);
	}
      } else {
	/* mapsql is not a select statment */
	MU_PRINTBUF("%s\n", mapsql);
      }
    }
  }

  if (cursor>bufsize){
    free(buf);
    MU_WARN("%s\n", "Oops! A buffer overflow was prevented while constructing the command files derived from the map query.  Unfortunately that buffer ran out of space.  The query was not run.");
    return -1;
  }

  FILE * qcfile = mu_fopen(fname,"w");
  if (NULL==qcfile){
    free(buf);
    return -1;
  }
  int bytes_written = fputs(buf, qcfile);
  free(buf);
  if (bytes_written<0){
    MU_WARN_FNAME(fname);
    MU_WARN_IF_ERRNO();
    return -1;
  }
  MU_FCLOSE_W(fname, -1, qcfile);
  return 0;
}

char * mu_run_query(struct mu_DBCONF *conf, struct mu_QUERY *q)
{

  if (NULL==conf){
    MU_WARN("%s\n", mu_error_null_dbconf);
    return NULL;
  }

  if (NULL==q){
    MU_WARN("%s\n", mu_error_null_query);
    return NULL;
  }

  /* unpack the query. check existence of mapsql, always required. */

  const char *mapsql = q->mapsql;
  if (NULL==mapsql){
    MU_WARN("%s\n", mu_error_null_query);
    return NULL;
  }

  const char *reducesql = q->reducesql;
  const char *createtablesql = q->createtablesql;

  const char *tmpdir = mu_create_temp_dir();
  if (NULL==tmpdir)
    return NULL;

  int icore;

  struct mu_SQLITE3_TASK *mapsql_task[conf->ncores];
  for(icore=0;icore<conf->ncores;++icore){
    mapsql_task[icore] =
      mu_define_task(tmpdir, NULL, "mapsql", icore);
    if (NULL==mapsql_task[icore])
      return NULL;
  }

  struct mu_SQLITE3_TASK *reducesql_task =
    mu_define_task(tmpdir,mapsql_task[0]->dbname,"reducesql",0);
  if (NULL==reducesql_task)
    return NULL;

  FILE *reducef = NULL;
  const char * rname = reducesql_task->iname;

  size_t cursor = 0; // for reduce
  size_t bufsize = (reducesql)? ((1024*conf->ncores)+strlen(reducesql)) :0;
  char *buf = NULL;

  const char *ext = mu_sqlite3_extensions();

#define MU_FREE_Q() do { \
    int i;							\
    for(i=0;i<conf->ncores;++i){				\
      mu_free_task(mapsql_task[i]);				\
    }								\
    mu_free_task(reducesql_task);				\
    if (reducesql) free(buf);					\
    free((void *) tmpdir);					\
  } while(0)							\


  if (reducesql){
    buf = malloc(bufsize);
    if (NULL==buf){
      MU_WARN_OOM();
      MU_FREE_Q();
      return NULL;
    }
    MU_PRINTBUF("%s\n",".bail on");
    if (ext)
      MU_PRINTBUF("%s\n", ext);
  }

  const size_t coredbnamesize = 255;

  const char *errormsg_on_start = "Fatal error detected by mu_query() attempting to start sqlite3 ";
  const char *errormsg_on_finish_map = "Fatal error detected by mu_query() in map task";
  const char *errormsg_on_finish_reduce = "Fatal error detected by mu_query() in reduce task";

  for(icore=0;icore<(conf->ncores);++icore){
    if ((reducesql) && (icore>0)){
      MU_PRINTBUF("attach database '%s' as 'coredb%.3d';\n",
		  mapsql_task[icore]->dbname,
		  icore);
      MU_PRINTBUF("insert into %s select * from coredb%.3d.%s;\n",
		  conf->otablename,
		  icore,
		  conf->otablename);
      MU_PRINTBUF("detach database 'coredb%.3d';\n", icore);
    }
    int shardc = mu_getcoreshardc(icore, conf->ncores, conf->shardc);
    const char **shardv = mu_getcoreshardv(icore, conf->ncores, conf->shardc, conf->shardv);

    if (NULL==shardv){
      MU_FREE_Q();
      return NULL;
    }
    int makestatus = mu_makeQueryCoreFile(conf,
					  mapsql_task[icore]->iname,
					  mapsql_task[icore]->dbname,
					  shardc,
					  shardv,
					  mapsql);
    free(shardv);
    if (makestatus){
      MU_FREE_Q();
      return NULL;
    }
    if (mu_start_task(mapsql_task[icore], errormsg_on_start)){
      MU_FREE_Q();
      return NULL;
    }
  }

  char *result = NULL;

  if (reducesql){
    int reducer_status=0;
    reducef = mu_fopen(rname, "w");
    if (NULL==reducef){
      MU_FREE_Q();
      return NULL;
    }
    MU_PRINTBUF("%s\n", reducesql);
    if (cursor>bufsize){
      MU_WARN("%s\n", "An unusual error occurred.  The map query may have begun processing, but the reduce query was not processed.");
      MU_FREE_Q();
      return NULL;
    }
    int written = fputs(buf, reducef);
    if (written<=0){
      MU_WARN_FNAME(rname);
      MU_WARN_IF_ERRNO();
      MU_FREE_Q();
      return NULL;
    }
    MU_FCLOSE_W(rname, NULL, reducef);
  }

  // wait for workers

  for(icore=0;icore<conf->ncores;++icore){
    if (mu_finish_task(mapsql_task[icore], errormsg_on_finish_map)){
      MU_FREE_Q();
      return NULL;
    }
  }
  if (reducesql){
    if (mu_start_task(reducesql_task, errormsg_on_start)){
      MU_FREE_Q();
      return NULL;
    }
    if (mu_finish_task(reducesql_task, errormsg_on_finish_reduce)){
      MU_FREE_Q();
      return NULL;
    }
    result = mu_read_small_file(reducesql_task->oname);
  }
  mu_remove_temp_dir(tmpdir);
  MU_FREE_Q();
  return result;
}
