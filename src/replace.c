/* replace.c code snippet
 * Copyright 2015 Dr Paul Brewer Economic and Financial Technology Consulting LLC 
 * License: MIT
 * http://opensource.org/licenses/MIT   
 */

#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <argz.h>

char *replace_words(const char *txt, int words, const char * const *forests, const char * const *laughter){
  /* And its whispered by nerds after checking params to replace_words()  */
  /* libc is space splitting txt into tokens. */
  /* A new string will dawn from malloc it's drawn */
  /* and forests will instead echo laughter */

  /* note: strips extra (doubled or more) spaces */
  
  if (NULL==txt)
    return NULL;
  char *in = strdup(txt);
  if (NULL==in)
    return NULL;
  if ((words <=0) || (NULL==forests) || (NULL==laughter))
    return in;
  char *argz;
  size_t argzlen;
  const int sep=' ';
  int e = argz_create_sep(in, sep, &argz, &argzlen);
  if (e){
    free(in);
    return NULL;
  }
  int j;
  for(j=0;j<words;++j){
    unsigned int replace_count;
    if ( (forests[j]) && (laughter[j]) ){
      e = argz_replace(&argz, &argzlen, forests[j], laughter[j], &replace_count);
      if (e){
	free(in);
	return NULL;
      }
    }
  }
  argz_stringify(argz,argzlen,sep);
  if (argz != in){
    free(in);
  }
  return argz;
}


int main(int argc, char **argv){
  const char *a = argv[1];
  const char *b = argv[2];
  const char *c = argv[3];
  const char *d = argv[4];
  const char *fromword[2] = {a,c};
  const char *toword[2] = {b,d};
  char buf[2048];
  while (fgets(buf,2047,stdin)){
    char *out = replace_words(buf, 2, fromword, toword);
    printf("%s\n",out);
    free(out);
  }
}
