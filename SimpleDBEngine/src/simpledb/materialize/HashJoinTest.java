package simpledb.materialize;

import simpledb.index.Index;
import simpledb.index.planner.IndexJoinPlan;
import simpledb.metadata.IndexInfo;
import simpledb.metadata.MetadataMgr;
import simpledb.plan.Plan;
import simpledb.plan.TablePlan;
import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.record.RID;
import simpledb.record.TableScan;
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
//		useIndexManually(studentplan, enrollplan, sidIdx, "sid");
//		useIndexScan(studentplan, enrollplan, sidIdx, "sid");
		useBlockScan(tx, studentplan, enrollplan, "sid", "studentid");

		tx.commit();
	}

	private static void useIndexManually(Plan p1, Plan p2, IndexInfo ii, String joinfield) {
		// Open scans on the tables.
		Scan s1 = p1.open();
		TableScan s2 = (TableScan) p2.open();  //must be a table scan
		Index idx = ii.open();

		// Loop through s1 records. For each value of the join field, 
		// use the index to find the matching s2 records.
		while (s1.next()) {
			Constant c = s1.getVal(joinfield);
			idx.beforeFirst(c);
			while (idx.next()) {
				// Use each datarid to go to the corresponding Enroll record.
				RID datarid = idx.getDataRid();
				s2.moveToRid(datarid);  // table scans can move to a specified RID.
				System.out.println(s2.getString("grade"));
			}
		}
		idx.close();
		s1.close();
		s2.close();
	}

	private static void useIndexScan(Plan p1, Plan p2, IndexInfo ii, String joinfield) {
		// Open an index join scan on the table.
		Plan idxplan = new IndexJoinPlan(p1, p2, ii, joinfield);
		Scan s = idxplan.open();

		while (s.next()) {
			System.out.println(s.getString("grade"));
		}
		s.close();
	}

	private static void useBlockScan(Transaction tx, Plan p1, Plan p2, String joinfield1, String joinfield2) {
		// Open an index join scan on the table.
		Plan blockplan = new HashJoinPlan(tx, p1, p2, joinfield1, joinfield2);
		Scan s = blockplan.open();

		while (s.next()) {
			System.out.println(s.getString("sname") + " " + s.getString("grade"));
		}
		s.close();
	}
}