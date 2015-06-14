# multicoresql
### A parallel map/reduce execution framework for sqlite databases

Dr Paul Brewer - Economic and Financial Technology Consulting LLC - drpaulbrewer@eaftc.com

`multicoresql` speeds up sql by executing queries on the multiple cores in a single computer across multiple 
database shards. It has been tested on 2/4/8 core consumer hardware and the 16 and 32 core rentals available
at Amazon EC2 (e.g. c4-8xlarge).  

The primary consideration for good performance is that the dataset fit into Linux cache memory.  
multicoresql does not require that datasets fit into memory and will happily slog through larger datasets, although
performance will be limited by disk bottlenecks.  The time required reading disk vs memory can be 10-20x

multicoresql does not at this time distribute tasks across multiple machines. 

##License: [The MIT License](https://raw.githubusercontent.com/DrPaulBrewer/multicoresql/master/LICENSE.txt)

##Test Drive without Installation

If you have the [`docker`](http://docker.com) container service running on a Linux host, you can test drive 
multicoresql without installing it permanently.  multicoresql does not otherwise use or require docker.

The test drive will always fetch the latest code from this github site.

Run either of these docker commands for a test:

clang build and test:  `docker run -it drpaulbrewer/multicoresql-test-clang:latest`

gcc build and test:    `docker run -it drpaulbrewer/multicoresql-test-gcc:latest`

These containers are located on the public dockerhub and will be automatically downloaded by docker, then run
the build and a test script and finally exit.  

The test script gives several examples of map and reduce queries and expected results against
a sharded table of integers from 1 to 1,000,000.  This exercises creation of the shards from a csv file
and the map/reduce capability.  Among the tests are math queries that sum series for log(2) and PI.

For more control, run manually by appending `/bin/bash` to the `docker run` command.  Then, at the
root prompt for the container, `cd /opt/github` and `./fetchBuildTest.sh` to run the build and test script.  

##Installation

multicoresql uses and requires as pre-requsities the [SQLite](http://www.sqlite.org) database engine and the [scons](http://www.scons.org) build system

multicoresql prefers to compile under [`clang`](http://clang.llvm.org/) but will also compile under gcc and will install into `/usr/local`.

Install script for bare Debian and related distros such as Ubuntu:

    sudo apt-get install git scons clang sqlite3
    git clone https://github.com/DrPaulBrewer/multicoresql
    cd multicoresql
    # make the build directory where the compiled libraries and executables will be written
    mkdir ./build
    # scons version of classic "make"
    scons
    # scons version of classic "make install"
    sudo scons install
    
##Manifest

After running `sudo scons install` the following will be installed in `/usr/local`:

`/usr/local/lib/libmulticoresql.so` -- shared library of multicoresql functions

`/usr/local/bin/3sqls` -- query runner that always allocates queries over 3 Linux sqlite3 processes 

`/usr/local/bin/sqls`  -- query runner that allocates queries over a selectable number of processes, 
                            defaulting to 1 per core

`/usr/local/bin/sqlsfromcsv` --  from a csv data file and a schema,
                                builds a directory containing sqlite3 database shards  

`/usr/local/bin/sqlsfromsqlite` -- from an existing sqlite3 database table with a shardid column,
                                builds a directory containing sqlite3 database shards
    
##Importing Data

###from CSV

    sqlsfromcsv

is used to create, from a single CSV file, a collection of sqlite3 shard databases suitable for multicoresql.

Running `sqlsfromcsv` without parameters provides this reminder message:

    Usage: sqlsfromcsv csvfile skiplines schemafile tablename dbDir shardcount
    Example: sqlsfromcsv example.csv 1 createmytable.sql mytable ./mytable 100
    
`csvfile` String, is the /path/to/csvfile.csv

`skiplines` Number, is the number of lines to skip for omitting CSV header. Usually 1 or 0.

`schemafile` String, gives a filename or a string containing an sqlite `create table` statement for the table data 

`tablename` String, is the tablename to create.  

`dbDir` String, is the /path/to/shards for a directory where sqlsfromcsv should output the sqlite3 database shards

`shardcount` Number, is the number of sqlite3 database shards that should be created from the data in the csv file

Each data row from the csv file is sharded randomly to a shard using a random number generator to select the shard.

###from existing SQLite Database

    sqlsfromsqlite 
    
is used to create, from a table in an existing sqlite3 database with a shardid column, a collection of sqlite3 shards.

Running `sqlsfromsqlite` without parameters provides this reminder message:
    
    usage: sqlsfromsqlite <dbname> <tablename> <dbdir> 

`<dbname>` String, is the /path/to/an/existing/sqlite3.db 

`<tablename>` String, is the source table for extracting data. Currently each run is restricted to a single table.

`<dbDir>` String, is the /path/to/shards for a directory where sqlsfromsqlite should output the sqlite3 db shards

shard databases are created and named from the distinct values of the `shardid` column of the input table.

Allowed characters in the `shardid` column are `[0-9][A-Z][a-z].-_` alphanumeric, dot, dash, and underscore; 
except that  dot is illegal as the first character of a `shardid`.  

##Running Queries

###Map/Reduce

Quick Example: Summing a column

    sqls -d ./myshards -m "select sum(n) as partsum from mytable;"  -r "select sum(partsum) as fullsum from maptable;"

Notice here:

`-d` is used to identify a directory containing sqlite3 databases (shards)

`-m` is used to identify a *map query* that is run on each shard database; 

plus, optionally

`-r` is used to identify a *reduce query* that is run on the data collected from the map query.  

The reduce query is always written against the table `maptable`.  

`maptable` is created by multicoresql as the collected results of running the map query on each shard.

###Map Only

For a map query only the 

`-d` (sqlite3 db shard directory) and 
`-m` (mapped sqlite3 query string or sql filename) 

parameters are required.

###Output formats

Queries are interpreted by the sqlite3 command line shell, and therefore all output formats
of sqlite3 defined in the sqlite3 `.mode` command are supported:

From the sqlite3 documentation: 

`.mode MODE ?TABLE? `   Set output mode where MODE is one of:

`ascii`    Columns/rows delimited by 0x1F and 0x1E

`csv`      Comma-separated values

`column`   Left-aligned columns.  (See .width)

`html`     HTML `<table>` code

`insert`   SQL insert statements for TABLE

`line`     One value per line

`list`     Values delimited by .separator strings

`tabs`     Tab-separated values

`tcl`      TCL list elements

See https://www.sqlite.org/cli.html for a complete list of special commands.

Sqlite3 `.mode` commands would typically be given in the `-r` *reduce query*.    

You may provide multiple sql commands by using ";" as a separator 

##Options

###Environment Variables

`MULTICORE_SQLITE3_BIN` specify the `/path/to/usr/local/bin/sqlite3` path to the sqlite3 command shell.
The default is to search `PATH`.   Useful if you have multiple sqlite3 executables or need a special version.              

`MULTICORE_SQLITE3_EXTENSIONS` a space-separated list of libraries to be loaded by multicoresql 
via the sqlite3 `.load` command

###Temp Directories

multicoresql creates a temporary directories while running, in `/tmp/multicoresql-XXXXXX`
where X is an alphanumeric character.
    
Temporary directories are typically removed on successful completion of a query or command, but are left
behind by failed queries and commands.  This is by design, and allows for post-failure inspection.
    
FAQ
===

## Common Bugs, Issues, and Error Messages

###FAQ 1. Can not run `sqls`, `sqlsfromcsv` or other executables from build directory:
    
What you did: 

    cd /path/to/multicoresql
    mkdir ./build
    scons
    cd ./build
    # try to run anything in the build directory

Message:  `./sqls: error while loading shared libraries: libmulticoresql.so: cannot open shared object file: No such file or directory`

Issue:  The linker can not load the shared library libmulticoresql.so because it doesn't know where to look

Fix:

    export LD_LIBRARY_PATH=/path/to/multicoresql/build
    
See also: http://stackoverflow.com/questions/4754633/linux-program-cant-find-shared-library-at-run-time

###FAQ 2.  Can not run `sqls`, `sqlsfromcsv` or other executables after `sudo scons install` :

Message:  Similar to FAQ #1  

`error while loading shared libraries: libmulticoresql.so: cannot open shared object file: No such file or directory`

Issue:  The linker can not load the shared library `/usr/local/lib/libmulticoresql.so` because it doesn't know that it is available for system wide use.  

Fix:  Once the files are installed in `/usr/local` as `root`, refresh the ldconfig

    sudo ldconfig

See also:  http://askubuntu.com/questions/631275/how-do-i-do-this-install-you-may-need-to-run-ldconfig


