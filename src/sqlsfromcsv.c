/* sqlsfromcsv.c 
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




#include "multicoresql.h"

long int get_long_int_or_die(const char *instring, const char *errfmt){
  long int result = strtol(instring, NULL, 10);
  if ((0==result) && (errno)){
    fprintf(stderr, errfmt, instring);
    exit(EXIT_FAILURE);
  }
  return result;
}

int main(int argc, char **argv){
  if (argc!=7){
    fprintf(stderr,
	    "%s\n%s\n",
	    "Usage: sqlsfromcsv csvfile skiplines schemafile tablename dbDir shardcount",
	    "Example: sqlsfromcsv example.csv 1 createmytable.sql mytable ./mytable 100");
    exit(EXIT_FAILURE);
  }
  int skiplines = 0;
  int shardcount = 0;
  const char *csvname = argv[1];
  skiplines = (int) get_long_int_or_die(argv[2],
					   "sqlsfromcsv: parameter skiplines, expected number of lines to skip, got: %s \n");
  if (skiplines < 0)
    skiplines = -skiplines;
  const char *schemaname = argv[3];
  const char *tablename = argv[4];
  const char *dbDir = argv[5];
  shardcount = (int) get_long_int_or_die(argv[6],
					 "sqlsfromscsv: expected positive number of shards, got: %s \n");

  if (shardcount<0)
    shardcount = -shardcount;
  if (shardcount<3)
    shardcount=3;
  
  int status = mu_create_shards_from_csv(csvname,skiplines,schemaname,tablename,dbDir,shardcount);
  const char *err = mu_error_string();
  if (err)
    fputs(err,stderr);
  return status;
}
