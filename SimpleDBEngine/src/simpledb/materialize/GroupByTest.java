package simpledb.materialize;

import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.query.Scan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class GroupByTest {
   public static void main(String[] args) {
      try {
    	 // analogous to the driver
		 SimpleDB db = new SimpleDB("studentdb");
		
		 // analogous to the connection
		 Transaction tx  = db.newTx();
		 Planner planner = db.planner();

         // analogous to the statement
         String qry = "select majorid, count(sname), sum(sname), avg(sid), max(gradyear), min(gradyear)" +
                      " from STUDENT group by majorid";
         Plan p = planner.createQueryPlan(qry, tx);

         // analogous to the result set
         Scan s = p.open();

         System.out.println("Countofsname\tSumofsname\tAvgofsid\tMaxofgradyear\tMinofgradyear\tMajor");
         while (s.next()) {
            int count = s.getInt("countofsname"); //SimpleDB stores field names
            int sum = s.getInt("sumofsname"); //SimpleDB stores field names
            int avg = s.getInt("avgofsid"); //SimpleDB stores field names
            int max = s.getInt("maxofgradyear"); //SimpleDB stores field names
            int min = s.getInt("minofgradyear"); //SimpleDB stores field names
            int majorid = s.getInt("majorid"); //in lower case
            System.out.println(count + "\t\t\t\t" + sum + "\t\t\t" + avg + "\t\t\t" + max + "\t\t\t" + min + "\t\t\t" + majorid);
         }
         s.close();
         tx.commit();
         
      }
      catch(Exception e) {
         e.printStackTrace();
      }
   }
}
