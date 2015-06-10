#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(int argc, char ** argv)
{
  long int n;
  // const long int billion = 1000000000L;
  if (argc!=3){
    fprintf(stderr, "usage: LeibnizPi1G <fromidx> <toidx_uninclusive>\n");
    exit(EXIT_FAILURE);
  }
  long int loop_from = strtol(argv[1],NULL,10);
  long int loop_to = strtol(argv[2], NULL, 10);
  // fprintf(stderr,"%ld\n%ld\n", loop_from, loop_to);
  long double pisum = 0.0L;
  const long double one =  1.0L;
  const long double four = 4.0L;
  for(n=loop_from;n<loop_to;++n)
    pisum+=(one/(four*n+one))-(one/(four*n-one));
  long double LeibnizPi = 4.0*(1.0+pisum);
  printf("%.20Lf\n", LeibnizPi);
}
