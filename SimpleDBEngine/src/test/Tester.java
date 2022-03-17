package test;
import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.tx.Transaction;
import simpledb.plan.Planner;
import simpledb.server.SimpleDB;

public class Tester {
   public static void main(String[] args) {
      try {
    	 // analogous to the driver
		 SimpleDB db = new SimpleDB("studentdb");
		
		 // analogous to the connection
		 Transaction tx  = db.newTx();
		 Planner planner = db.planner();

         // analogous to the statement
//         String qry = "select majorid, count(majorid) from student group by majorid"; // completed
//         String qry = "select * from student"; //completed
//         String qry = "select *, count(sname), count(*) from student group by gradyear"; // completed
//         String qry = "select count(*) from student group by majorid"; // completed
//         String qry = "select count(distinct gradyear), majorid from student group by majorid";
//         String qry = "select sname from student where majorid = 20";
         String qry = "select sname, title, prof, grade from student, enroll, course, " +
                 "section where studentid = sid and sectionid = sectid and courseid = cid";
         Plan p = planner.createQueryPlan(qry, tx);

         // analogous to the result set
         Scan s = p.open();

         System.out.println("success");
         while (s.next());
//         System.out.println("Count of majorid\tmajorid\"");
//         while (s.next()) {
//            String count = s.getString("countofmajorid"); //in lower case
//            int count = s.getInt(("countofmajorid"));
//            int major = s.getInt(("majorid"));
//            System.out.println(count);
//            System.out.println(count + "\t" + major);
//         }
		 
//		 String qry = "create index majoridx "
//				 	+ "on STUDENT(MajorID) using hash";
//         int p = planner.executeUpdate(qry, tx);
         
//         if (p > 0) {
//        	 System.out.println("Something changed. Rows affected:" + p);
//         } else {
//        	 System.out.println("Nothing changed. Rows affected:" + p);
//         }
         s.close();
         tx.commit();
         
      }
      catch(Exception e) {
         e.printStackTrace();
      }
   }
}
