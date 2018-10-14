#!/usr/bin/env python
import os
import subprocess
import math
import time
import re

if not re.search(r'/(test)$', os.getcwd()):
    print "Please cd to the test directory before running the tests."
    exit()

    
print "test1G.py -- These tests require about 40G free disk space"
os.system("rm -rf ./giga")
os.system("rm -rf ./gigadata.csv");
print "Building billion number gigadata.csv file on single core.  This may take a few minutes."
os.system("./numbers 1 1000000000 > ./gigadata.csv");
os.putenv('LD_LIBRARY_PATH','../build')

def runsqls(mybin, db, mapsql, reducesql):
    return subprocess.check_output([mybin, "-d", db, "-m", mapsql, "-r", reducesql])

def test(mybin, db, mapsql, reducesql, expected, tol):
    print "Test:"
    print "  bin            "+mybin
    print "  db        (-d) "+db
    print "  mapsql    (-m) "+mapsql
    print "  reducesql (-r) "+reducesql
    t0 = time.time()
    got = runsqls(mybin,db,mapsql,reducesql).rstrip()
    t1 = time.time()
    print "  time (sec)     "+str(t1-t0)
    print "  expect         "+str(expected)+" +/- "+str(tol)
    print "  got            "+got
    gotr = float(got)
    if (abs(gotr-float(expected))<float(tol)):
        print "  result         "+"PASS"
    else:
        print "  result         "+"FAIL"
    print " "
    print "-------------------------------------------------"
    print " "

setupsqls = "../build/sqlsfromcsv gigadata.csv 0 gigadata.sql giga ./giga 100"
print "setting up ./giga test databases with :"
print setupsqls
print "Get a cup of coffee.  This may take a while!"
if os.system(setupsqls):
    print "sqlsfromcsv failed! failed to setup ./test/giga database directory. "
    exit()


def suite(mybin,db):
    m0 = "select n from gig where n>=1000 and n<=2000;"
    r0 = "select sum(n) from maptable;"
    e0 = (2000*2001/2)-(999*1000/2)
    t0 = 1
    test(mybin,db,m0,r0,e0,t0)

    m1 = "select sum(n) as sn from gig;"
    r1 = "select sum(sn) from maptable;"
    e1 = 1000000000*1000000001/2
    t1 = 1
    test(mybin,db,m1,r1,e1,t1)
    
    m2 = "select sum(1.0/(1.0+((1.0e-9)*n))) as z from gig;"
    r2 = "select (1.0e-9)*sum(z) as ln2 from maptable;"
    e2 = math.log(2.0)
    t2 = 0.00001
    test(mybin,db,m2,r2,e2,t2)
    
    m3 = "select sum( (1.0/(4.0*n+1.0))-(1.0/(4.0*n-1.0)) ) as pisum from gig;"
    r3 = "select 4.0*(1.0+sum(pisum)) as LeibnizPI from maptable;"
    e3 = math.pi
    t3 = 0.00001
    test(mybin,db,m3,r3,e3,t3)
    
    m4 = "select sum( (1.0/(n*n)) ) as recip2sum from gig;"
    r4 = "select sum(recip2sum) as EulerPi2Over6 from maptable;"
    e4 = math.pi*math.pi/6.0
    t4 = 0.00001
    test(mybin,db,m4,r4,e4,t4)
    

suite("../build/sqls", "./giga")
suite("../build/3sqls", "./giga")
 
os.system("rm -rf ./giga")
os.system("rm -rf ./gigadata.csv");


