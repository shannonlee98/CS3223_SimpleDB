package simpledb.materialize;

import simpledb.tx.Transaction;
import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.query.*;
import simpledb.server.SimpleDB;

/* This is a version of the StudentMajor program that
 * accesses the SimpleDB classes directly (instead of
 * connecting to it as a JDBC client).
 *
 * These kind of programs are useful for debugging
 * your changes to the SimpleDB source code.
 */

public class AggregateTest {
    public static void main(String[] args) {
        try {
            // analogous to the driver
            SimpleDB db = new SimpleDB("studentdb");

            // analogous to the connection
            Transaction tx = db.newTx();
            Planner planner = db.planner();

            //_______________________________________________________________
            String qry1 = "select sid, majorid from student";
            Plan p1 = planner.createQueryPlan(qry1, tx);

            // analogous to the result set
            Scan s1 = p1.open();

            System.out.println("majorid\t\tsid");
            while (s1.next()) {
                int sid = s1.getInt("sid");
                int majorid = s1.getInt("majorid"); //SimpleDB stores field names
                System.out.println(majorid + "\t" + sid);
            }
            System.out.println("\n______________________________\n");
            //_______________________________________________________________
            String qry = "select majorid, MAX(sid), MIN(sid), AVG(sid), COUNT(sid), SUM(sid) from student group by majorid";
//            String qry = "select sid, majorid from student";
            Plan p = planner.createQueryPlan(qry, tx);

            // analogous to the result set
            Scan s = p.open();

            System.out.println("majorid\tmaxofsid\tminofsid\tavgofsid\tcoutofsid\tsumofsid");
//            System.out.println("sid\t\tmajorid");
            while (s.next()) {
                int minofsid = s.getInt("minofsid");
                int maxofsid = s.getInt("maxofsid");
                int avgofsid = s.getInt("avgofsid");
                int countofsid = s.getInt("countofsid");
                int sumofsid = s.getInt("sumofsid");
                int majorid = s.getInt("majorid"); //SimpleDB stores field names
                System.out.println(majorid + "     \t" + maxofsid + "        \t" + minofsid + "       \t" + avgofsid + "       \t" + countofsid + "         \t" + sumofsid);
            }
            s.close();
            s1.close();
            tx.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
