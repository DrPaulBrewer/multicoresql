#include "libmulticoresql.h"

int main(int argc, char **argv){
  if (argc<4){
    fprintf(stderr,"%s\n","usage: sqlshard <dbname> <tablename> <dbdir> \n");
    exit(EXIT_FAILURE);
  }

  return mu_create_shards_from_sqlite_table(argv[1], argv[2], argv[3]);
  
}
