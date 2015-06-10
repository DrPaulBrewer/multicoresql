#include <stdio.h>
#include <stdlib.h>
int main(int argc, char **argv){
  if (argc<3){
    fprintf(stderr,"%s\n","usage: numbers low high, e.g. numbers 1 10 produces 1..10 \n");
    exit(EXIT_FAILURE);
  }
  long int l = strtol(argv[1], NULL, 10);
  long int h = strtol(argv[2], NULL, 10);
  long int j;
  for(j=l;j<=h;++j)
    printf("%li\n", j);
  exit(EXIT_SUCCESS);
}
