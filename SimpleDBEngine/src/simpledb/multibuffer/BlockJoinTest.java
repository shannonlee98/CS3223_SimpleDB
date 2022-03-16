package simpledb.multibuffer;

import simpledb.index.Index;
import simpledb.index.planner.IndexJoinPlan;
import simpledb.metadata.IndexInfo;
import simpledb.metadata.MetadataMgr;
import simpledb.plan.Plan;
import simpledb.plan.TablePlan;
import simpledb.query.CondOp;
import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.record.RID;
import simpledb.record.TableScan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

// Find the grades of all students.

public class BlockJoinTest {
	public static void main(String[] args) {
		SimpleDB db = new SimpleDB("studentdb");
		MetadataMgr mdm = db.mdMgr();
		Transaction tx = db.newTx();

		// Get plans for the Student and Enroll tables
		Plan studentplan = new TablePlan(tx, "student", mdm);
		Plan enrollplan = new TablePlan(tx, "enroll", mdm);

		useBlockJoin(tx, studentplan, enrollplan, "sid", "studentid");

		tx.commit();
	}

	private static void useBlockJoin(Transaction tx, Plan p1, Plan p2, String joinfield1, String joinfield2) {
		// Open a block join scan on the table.
		Plan blockplan = new BlockJoinPlan(tx, p1, p2, joinfield1, new CondOp(CondOp.types.equals), joinfield2);
		Scan s = blockplan.open();

		while (s.next()) {
			System.out.println(s.getString("sname") + " " + s.getString("grade"));
		}
		s.close();
	}
}
