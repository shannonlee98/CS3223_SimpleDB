package simpledb.hash;

import simpledb.metadata.IndexInfo;
import simpledb.metadata.MetadataMgr;
import simpledb.plan.Plan;
import simpledb.plan.TablePlan;
import simpledb.query.Scan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.util.Map;

// Find the grades of all students.

public class HashJoinTest {
	public static void main(String[] args) {
		SimpleDB db = new SimpleDB("studentdb");
      MetadataMgr mdm = db.mdMgr();
      Transaction tx = db.newTx();

		// Find the index on StudentId.
		Map<String,IndexInfo> indexes = mdm.getIndexInfo("enroll", tx);
		IndexInfo sidIdx = indexes.get("studentid");

		// Get plans for the Student and Enroll tables
		Plan studentplan = new TablePlan(tx, "student", mdm);
		Plan enrollplan = new TablePlan(tx, "enroll", mdm);

		// Two different ways to use the index in simpledb:
		useHashScan(tx, studentplan, enrollplan, "sid", "studentid");

		tx.commit();
	}

	private static void useHashScan(Transaction tx, Plan p1, Plan p2, String joinfield1, String joinfield2) {
		// Open an index join scan on the table.
		Plan blockplan = new GraceHashJoinPlan(tx, p1, p2, joinfield1, joinfield2);
		Scan s = blockplan.open();

		while (s.next()) {
			System.out.println(s.getString("sname") + " " + s.getString("grade"));
		}
		s.close();
	}
}
