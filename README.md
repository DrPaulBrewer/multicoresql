# multicoresql
## A parallel map/reduce execution framework for sqlite databases

Dr Paul Brewer - Economic and Financial Technology Consulting LLC - drpaulbrewer@eaftc.com

`multicoresql` speeds up sql by executing queries on the multiple cores in a single computer across multiple 
database shards. It has been tested on 2/4/8 core consumer hardware and the 16 and 32 core rentals available
at Amazon EC2 (e.g. c4-8xlarge).  

THe primary consideration for good performance is that the dataset fit into Linux cache memory.  

multicoresql does not require that datasets fit into memory and will happily slog through larger datasets, although
performance will be limited by disk bottlenecks.  The time required reading disk vs memory can be 10-20x

multicoresql does not at this time distribute tasks across multiple machines. 

multicoresql has import utilities to create shards.  

The end user is required to rewrite their SQL queries to work with the sharded databases as follows:

a *map query* that is run on each shard database; 

plus, optionally

a *reduce query* that is run on the data collected from the map query

Quick Example: Summing a column

    sqls -d ./myshards -m "select sum(n) as partsum from mytable;"  -r "select sum(partsum) as fullsum from maptable;"

##License: [The MIT License](https://raw.githubusercontent.com/DrPaulBrewer/multicoresql/master/LICENSE.txt)

##Installation

multicoresql uses and requires as pre-requsities the [SQLite](http://www.sqlite.org) database engine and the [scons](http://www.scons.org) build system

multicoresql is setup to compile under [`clang`](http://clang.llvm.org/) and install into `/usr/local`

If you'd prefer instead to compile with gcc, change `CC` on the top line of  ./src/SConscript before running `scons`

Install script for Debian and related distros such as Ubuntu:

    sudo apt-get install scons sqlite3
    # clang-3.6 needed if you want to compile as is using the clang-3.6 compiler 
    sudo apt-get install clang-3.6   
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

###from existing SQLite Database

##Running Queries

###Map/Reduce

###Map Only

###Output formats

##Options

###Environment Variables

####SQLITE3 Extension Libraries

###Temp Directories

