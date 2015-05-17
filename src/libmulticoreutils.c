/* libmulticoreutils.c Copyright 2015 Paul Brewer
 * Economic and Financial Technology Consulting LLC
 * All rights reserved.
 * common routines for all versions of multicoresql
 */

#include "libmulticoreutils.h"

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
    exit(1);
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

pid_t mu_run(const char *bin, char * const* argv, const char *infile, const char *outfile){
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
  
  if (stat(fname, &fstats)!=0)
    return NULL;
  
  if (errno!=0)
    return NULL;

  FILE *f = fopen(fname,"r");
  
  if (errno!=0)
    return NULL;

  size_t buflen = (size_t) fstats.st_size + 1;

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
		       const char *oname
		       ){
  if (status!=0){
    fprintf(stderr,"%s :\nBad exit status %d from \n%s < %s > %s \n",msg,status,bin,iname,oname);
  }
  return status;
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
  FILE *getcmdf = mu_fopen(getshardnamesin, "w");
  fprintf(getcmdf, shard_setup_fmt, tablename);
  fclose(getcmdf);
  int runstatus=0;
  waitpid(mu_run(sqlite3_bin, argv, getshardnamesin, getshardnamesout), &runstatus, 0);
  mu_warn_run_status("Warning: mu_create_shards_from_sqlite_table()",
		     runstatus,
		     sqlite3_bin,
		     getshardnamesin,
		     getshardnamesout);
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
  FILE *makeshardsf = mu_fopen(makeshardsin, "w");
  for(i=0;i<shardc;++i){
    if (ok_mu_shard_name(shardv[i]))
      fprintf(makeshardsf, shard_data_fmt, dbdir, shardv[i], tablename, tablename, shardv[i]);
  }
  fclose(makeshardsf);
  int shardstatus = 0;
  waitpid(mu_run(sqlite3_bin, argv, makeshardsin, makeshardsout), &shardstatus, 0);
  mu_warn_run_status("Warning: mu_create_shards_from_sqlite_table() ",
		     shardstatus,
		     sqlite3_bin,
		     makeshardsin,
		     makeshardsout);
  free((void *) makeshardsin);
  free((void *) makeshardsout);
  free((void *) cmdout);
  free((void *) getshardnamesin);
  free((void *) getshardnamesout);
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
  pid_t pid = mu_run(sqlite3_bin, argv, cmdfname, NULL);
  int status;
  waitpid(pid, &status, 0);
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

