### Bonus Done:
Created Query Optimiser to decide which join to use.

### Plan to do:
Implement error-checking for queries

### Test Plan:
create test cases to test all join algorithms (where each join algorithm is prefered)
test non-equi join for nested loop & merge join
create a long query to see how query plan will look like

#### Example settings for join:
`setting 'cost'`  
`setting 'block'`  
`setting 'index'`  
`setting 'merge'`  
`setting 'hash'`  
`setting 'product'`  

#### 2 table equi-joins:
`select sname, grade from student, enroll where studentid = sid order by sname asc, grade asc`   
`select sname,dname from dept, student where majorid=did`

#### 2 table non-equi-joins:
`select sname, grade from student, enroll where studentid < sid order by sname asc, grade asc`

#### 4 table equi-joins:
`select sname, title, prof, grade 
from student, enroll, course, section 
where studentid = sid 
and sectionid = sectid 
and courseid = cid`