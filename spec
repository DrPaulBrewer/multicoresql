multi DB

Pros:  easier to understand
Cons:  multiple files.  dbs as directories.

CREATE  create table x as select * from y where blah;  OK
UPDATE  update x set foo=bar where prop>23;  OK
SELECT  select max(value) from x where prop>23; OK
DELETE  delete from x where prop>100;  OK
INSERT/SELECT ; OK

JOINS ... non-trivial

single DB -- multi table

Pros:  single file, a little faster
cons:  have to map statments.  lots of C string manipulation

CREATE  create table x as select something from other table where blah;  
	       rewrite x's ,  split across tables

UPDATE  update x set foo=bar where prop>23; 
	       rewrite, split across tables

SELECT  