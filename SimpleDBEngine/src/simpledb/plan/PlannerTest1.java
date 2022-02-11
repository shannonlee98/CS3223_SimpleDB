package simpledb.plan;

import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.query.Scan;

public class PlannerTest1 {
   public static void main(String[] args) {
      SimpleDB db = new SimpleDB("plannertest1");
      Transaction tx = db.newTx();
      Planner planner = db.planner();
      String cmd = "create table T1(A int, C int)";
      planner.executeUpdate(cmd, tx);

      int n = 200;
      System.out.println("Inserting " + n + " random records.");
      for (int i=0; i<n; i++) {
         int a = (int) Math.round(Math.random() * 50);
//         String b = "rec" + a;
         int c = (int) Math.round(Math.random() * 10);
         cmd = "insert into T1(A,C) values(" + a + ", " + c + ")";
         planner.executeUpdate(cmd, tx);
      }

      String qry = "select A, C from T1 where A < 50 order by A asc, C desc";
      Plan p = planner.createQueryPlan(qry, tx);
      Scan s = p.open();
      while (s.next())
         System.out.println(s.getInt("a") + " | "+  s.getInt("c"));
      s.close();;

      cmd = "delete from T1 where A <= 50";
      planner.executeUpdate(cmd, tx);

      tx.commit();
   }
}

