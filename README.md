# multicoresql
## A parallel execution framework for sqlite

Dr Paul Brewer - Economic and Financial Technology Consulting LLC - drpaulbrewer@eaftc.com

##License: [The MIT License](https://raw.githubusercontent.com/DrPaulBrewer/multicoresql/master/LICENSE.txt)

##Installation

multicoresql uses the SQLite database engine and the scons build system

It is setup to compile under clang and install into /usr/local

If you'd prefer to compile with gcc, change `CC` on the top line of  ./src/SConscript before running `scons`

Install script for Debian and related distros such as Ubuntu:

    sudo apt-get install scons sqlite3
    # clang-3.6 needed if you want to compile as is using the clang-3.6 compiler 
    sudo apt-get install clang-3.6   
    git clone https://github.com/DrPaulBrewer/multicoresql
    cd multicoresql
    # scons version of "make" step
    scons
    # scons version of classic "make install" step
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

