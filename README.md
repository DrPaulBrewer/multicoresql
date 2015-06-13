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

These containers will run the build and a test script and then exit.  

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

`-m` is used to identify a *map query* that is run on each shard database; 

plus, optionally

`-r` is used to identify a *reduce query* that is run on the data collected from the map query.  

The reduce query is always written against the table `maptable`.  

`maptable` is created by multicoresql as the collected results of running the map query on each shard.

###Map Only

###Output formats

##Options

###Environment Variables

####SQLITE3 Extension Libraries

###Temp Directories

