package simpledb.materialize;

import simpledb.display.ExecutionChain;
import simpledb.display.Join;
import simpledb.plan.Plan;
import simpledb.query.CondOp;
import simpledb.query.Scan;
import simpledb.record.Schema;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

/** The Plan class corresponding to the <i>indexjoin</i>
  * relational algebra operator.
  * @author Edward Sciore
  */
public class SimpleJoinPlan implements Plan {
   private Plan p1, p2;
   private Transaction tx;
   private String joinfield1;
   private String joinfield2;
   private Schema sch = new Schema();

   /**
    * Implements the join operator,
    * using the specified LHS and RHS plans.
    * @param tx the current transaction
    * @param p1 the left-hand plan
    * @param p2 the right-hand plan
    * @param joinfield1 the left-hand field used for joining
    * @param joinfield2 the right-hand field used for joining
    */
   public SimpleJoinPlan(Transaction tx, Plan p1, Plan p2, String joinfield1, String joinfield2) {
      this.p1 = p1;
      this.p2 = p2;
      this.tx = tx;
      this.joinfield1 = joinfield1;
      this.joinfield2 = joinfield2;
      sch.addAll(p1.schema());
      sch.addAll(p2.schema());
   }
   
   /**
    * Opens an indexjoin scan for this query
    * @see Plan#open()
    */
   public Scan open() {
      Scan ts1 = p1.open();

      // throws an exception if p2 is not a tableplan
      TableScan ts2 = (TableScan) p2.open();
      return new SimpleJoinScan(tx, ts1, joinfield1, ts2, joinfield2);
   }
   
   /**
    * Estimates the number of block accesses to compute the join.
    * The formula is:
    * <pre> B(indexjoin(p1,p2,idx)) = B(p1) + R(p1)*B(idx)
    *       + R(indexjoin(p1,p2,idx) </pre>
    * @see Plan#blocksAccessed()
    */
   public int blocksAccessed() {
      int records1 = p1.recordsOutput();
      int records2 = p2.recordsOutput();
      Plan outer = (records1 < records2) ? p1 : p2;
      Plan inner = (records1 < records2) ? p2 : p1;
      return outer.recordsOutput()
         + (outer.blocksAccessed() * inner.recordsOutput());
   }
   
   /**
    * Estimates the number of output records in the join.
    * The formula is:
    * <pre> R(indexjoin(p1,p2,idx)) = R(p1)*R(idx) </pre>
    * @see Plan#recordsOutput()
    */
   public int recordsOutput() {
      return p1.recordsOutput() /** ii.recordsOutput()*/;
   }
   
   /**
    * Estimates the number of distinct values for the 
    * specified field.  
    * @see Plan#distinctValues(String)
    */
   public int distinctValues(String fldname) {
      if (p1.schema().hasField(fldname))
         return p1.distinctValues(fldname);
      else
         return p2.distinctValues(fldname);
   }
   
   /**
    * Returns the schema of the index join.
    * @see Plan#schema()
    */
   public Schema schema() {
      return sch;
   }

   public ExecutionChain GetEC() {
      return new Join(this, p1.GetEC(), p2.GetEC(), joinfield1,
              new CondOp(CondOp.types.equals).toString(), joinfield2);
   }
}
