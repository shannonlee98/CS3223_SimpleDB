package test;
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
         String qry = "select SName from STUDENT where SName='alice' ";
//                    + "set MajorId=10 "
//                    + "where SName > 'art'";
		 
//		 String qry = "create index majoridx "
//				 	+ "on STUDENT(MajorID) using hash";
         int p = planner.executeUpdate(qry, tx);
         
         if (p > 0) {
        	 System.out.println("Something changed. Rows affected:" + p);
         } else {
        	 System.out.println("Nothing changed. Rows affected:" + p);
         }
         
         tx.commit();
         
      }
      catch(Exception e) {
         e.printStackTrace();
      }
   }
}
