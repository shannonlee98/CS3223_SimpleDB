package simpledb.materialize;

import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.query.Scan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.sql.Types;

public class SortTest {
   public static void main(String[] args) {
      try {
    	 // analogous to the driver
		 SimpleDB db = new SimpleDB("studentdb");
		
		 // analogous to the connection
		 Transaction tx  = db.newTx();
		 Planner planner = db.planner();

         // analogous to the statement
         String qry = "select SName, majorid from STUDENT order by majorid desc, sname asc";
         Plan p = planner.createQueryPlan(qry, tx);

         // analogous to the result set
         Scan s = p.open();

         System.out.println("Name\tMajor");
         while (s.next()) {
             String sname = s.getString("sname"); //SimpleDB stores field names
             int majorid = s.getInt("majorid"); //in lower case
             System.out.println(sname + "\t\t" + majorid);
         }
         s.close();
         tx.commit();
         
      }
      catch(Exception e) {
         e.printStackTrace();
      }
   }
}
