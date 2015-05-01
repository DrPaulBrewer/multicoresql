#include <stdio.h>
#include <stdlib.h>

char *foo;

int main(int argc, char **argv){
  int i;

  char **list = (char *[]){
    "/my/path/1",
    "/my/other/path/2",
    "another/3"
  };

  foo = getenv("foo");

  if (foo)
    printf("%s\n", foo);

  for(i=0;i<3;++i){
    printf("%s\n",list[i]);
  }
  
}
