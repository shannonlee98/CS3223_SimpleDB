### Bonus Done:

####Created Query Optimiser to decide which join to use.

#### Implement select all
`select * from student`  
`select count(*) from student`  
`select *, count(sname), count(*) from student group by gradyear`

#### Implement error-checking for queries:
Below are example queries which will trigger the error-checking exceptions

Missing table:  
`select sname from where majorid = 20`

Table doesn't exist:  
`select sname from ufo`  

Missing fields:  
`select from student`  

Field doesn't exist:  
`select alien from student`  
`select sname from student where alien > 100`  
`select sname from student order by alien`  
`select count(sname) from student group by alien`  

Wrong number of arguments in aggregation function:  
`select count() from student`   
`select count(sname, gradyear) from student`

Selecting field without including it in the group by clause when there are aggregate functions:  
`select sname, count(sname) from student`



### Test Plan:
create test cases to test all join algorithms (where each join algorithm is prefered)
test non-equi join for nested loop & merge join
create a long query to see how query plan will look like

#### Examples for Index Join to be Cheapest
1. `select sname, grade from student, enroll where studentid = sid and studentid = 30 order by sname asc, grade asc`
2. `select sname, grade from student, enroll where studentid = sid and studentid = 2000 order by sname asc, grade asc`

#### 2 table equi-joins:
1. `select sname, grade from student, enroll where studentid = sid order by sname asc, grade asc`   
2. `select sname,dname from dept, student where majorid=did`

#### 2 table non-equi-joins:
1. `select sname, grade from student, enroll where studentid < sid order by sname asc, grade asc`

#### 4 table equi-joins:
1. `select sname, title, prof, grade 
from student, enroll, course, section 
where studentid = sid 
and sectionid = sectid 
and courseid = cid`

### Example settings for join:
1. `setting 'cost'`
2. `setting 'block'`
3. `setting 'index'`
4. `setting 'merge'`
5. `setting 'hash'`
6. `setting 'product'` 

### Example settings for print:
1. Print results and display query plan  
   `setting 'printall'`
2. Print result but do not display query plan  
   `setting 'printresult'` 
3. Print neither results nor display query plan  
   `setting 'printnone'`
