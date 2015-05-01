#include <errno.h>
#include <stdio.h>
#include <stdlib.h>



void main(int argc, char **argv){
  errno = 0;
  printf("%s\n","Hello");
  char temptemplate[] = "/tmp/testXXXXXX";
  char *name = mkdtemp(temptemplate);
  printf("%i\n", errno);
  if (errno!=0) perror(NULL);
  printf("%s\n", name);
  
}
