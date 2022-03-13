package simpledb.display;

import simpledb.plan.Plan;

//[Table Join(sname, grade) Table]
//select sname, grade from student, enroll where studentid = sid
//select sname,dname from dept, student where majorid=did
//select sname,dname from dept, student where majorid=did and sid != did
public class Join implements ExecutionChain{
    ExecutionChain left;
    ExecutionChain right;
    String leftJoinField;
    String rightJoinField;
    Plan main;

    public Join(Plan main, ExecutionChain left,
                ExecutionChain right, String leftJoinField, String rightJoinField) {
        this.left = left;
        this.right = right;
        this.leftJoinField = leftJoinField;
        this.rightJoinField = rightJoinField;
        this.main = main;
    }

    @Override
    public String getName() {
        String rawName = main.getClass().getName();
        int start = rawName.lastIndexOf(".");
        return rawName.substring(start+1).replace("Plan", "").replace("Join", "JOIN");
    }

    @Override
    public String display() {
        String joininfo = leftJoinField == "" || leftJoinField == null ? "" : "(" + leftJoinField +
                "=" + rightJoinField + ")";
        return   "[" + left.display() + " " + this.getName() + joininfo + " " + right.display() + "]";
    }

    @Override
    public int cost() {
        return main.blocksAccessed();
    }
}
