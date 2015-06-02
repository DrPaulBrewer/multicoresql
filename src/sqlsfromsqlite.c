#include "multicoresql.h"

int main(int argc, char **argv){
  if (argc<4){
    fprintf(stderr,"%s\n","usage: sqlshard <dbname> <tablename> <dbdir> \n");
    exit(EXIT_FAILURE);
  }

  int status =  mu_create_shards_from_sqlite_table(argv[1], argv[2], argv[3]);
  const char *err = mu_error_string();
  if (err)
    fputs(err,stderr);
  return status;
}
