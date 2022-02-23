package simpledb.materialize;

import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.record.Schema;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

/**
 * The Plan class corresponding to the <i>indexjoin</i>
 * relational algebra operator.
 *
 * @author Edward Sciore
 */
public class HashJoinPlan implements Plan {
   private Plan outer, inner;
   private Transaction tx;
   private String joinfield1;
   private String joinfield2;
   private Schema sch = new Schema();

   /**
    * Implements the join operator,
    * using the specified LHS and RHS plans.
    *
    * @param tx         the current transaction
    * @param p1         the left-hand plan
    * @param p2         the right-hand plan
    * @param joinfield1 the left-hand field used for joining
    * @param joinfield2 the right-hand field used for joining
    */
   public HashJoinPlan(Transaction tx, Plan p1, Plan p2, String joinfield1, String joinfield2) {
      int records1 = p1.recordsOutput();
      int records2 = p2.recordsOutput();

      if (records1 < records2) {
         inner = p2;
         outer = p1;
         this.joinfield1 = joinfield1;
         this.joinfield2 = joinfield2;
      } else {
         inner = p1;
         outer = p2;
         this.joinfield1 = joinfield2;
         this.joinfield2 = joinfield1;
      }
      this.tx = tx;
      sch.addAll(p1.schema());
      sch.addAll(p2.schema());
   }

   /**
    * Opens an indexjoin scan for this query
    *
    * @see Plan#open()
    */
   public Scan open() {
      TableScan tsOuter = (TableScan) outer.open();
      TableScan tsInner = (TableScan) inner.open();

      // throws an exception if p1 is not a tableplan
      return new HashJoinScan(tx, tsOuter, joinfield1, tsInner, joinfield2);
   }

   /**
    * Estimates the number of block accesses to compute the join.
    * The formula is:
    * <pre> B(indexjoin(p1,p2,idx)) = B(p1) + R(p1)*B(idx)
    *       + R(indexjoin(p1,p2,idx) </pre>
    *
    * @see Plan#blocksAccessed()
    */
   public int blocksAccessed() {
      return outer.recordsOutput()
              + (outer.blocksAccessed() * inner.recordsOutput());
   }

   /**
    * Estimates the number of output records in the join.
    * The formula is:
    * <pre> R(indexjoin(p1,p2,idx)) = R(p1)*R(idx) </pre>
    *
    * @see Plan#recordsOutput()
    */
   public int recordsOutput() {
      return outer.recordsOutput() * inner.recordsOutput();
   }

   /**
    * Estimates the number of distinct values for the
    * specified field.
    *
    * @see Plan#distinctValues(String)
    */
   public int distinctValues(String fldname) {
      if (outer.schema().hasField(fldname))
         return outer.distinctValues(fldname);
      else
         return inner.distinctValues(fldname);
   }

   /**
    * Returns the schema of the index join.
    *
    * @see Plan#schema()
    */
   public Schema schema() {
      return sch;
   }
}