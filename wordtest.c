#include <stdio.h>
#include <stdlib.h>
#include <wordexp.h>

void main(int argc, char **argv){
  const char *glob = "./test/[0-9]*";
  wordexp_t p;
  int flags = WRDE_NOCMD;
  int x = wordexp(glob, &p, flags);
  int i;
  if (x==0){
    printf("%s\n","Matched ./test/[0-9]*");
    printf("found %zd matches \n", p.we_wordc);
    for(i=0;i<p.we_wordc;++i)
      printf("match %d is %s\n",i,p.we_wordv[i]);
  } 
}
