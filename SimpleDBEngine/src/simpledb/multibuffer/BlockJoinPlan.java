package simpledb.multibuffer;

import simpledb.display.ExecutionChain;
import simpledb.display.Join;
import simpledb.materialize.MaterializePlan;
import simpledb.materialize.TempTable;
import simpledb.plan.Plan;
import simpledb.query.CondOp;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

/**
 * The Plan class for the multi-buffer version of the
 * <i>product</i> operator.
 * @author Edward Sciore
 */
public class BlockJoinPlan implements Plan {
   private Transaction tx;
   private Plan inner, outer;
   private Schema schema = new Schema();
   private CondOp condOp;
   private String joinfieldInner, joinfieldOuter;

   /**
    * Creates a product plan for the specified queries.
    * @param p1 the plan for the LHS query
    * @param p2 the plan for the RHS query
    * @param tx the calling transaction
    */
   public BlockJoinPlan(Transaction tx, Plan p1, Plan p2, String joinfield1, CondOp condOp, String joinfield2) {
      this.tx = tx;
      int records1 = p1.recordsOutput();
      int records2 = p2.recordsOutput();

      if (records1 < records2) {
         inner = p2;
         outer = p1;
         this.joinfieldOuter = joinfield1;
         this.joinfieldInner = joinfield2;
         this.condOp = condOp;
      } else {
         inner = p1;
         outer = p2;
         this.joinfieldOuter = joinfield2;
         this.joinfieldInner = joinfield1;
         this.condOp = condOp.flip();
      }

      //might need to use materialise plan for some reason
//      this.lhs = new MaterializePlan(tx, lhs);

      schema.addAll(p1.schema());
      schema.addAll(p2.schema());
   }

   /**
    * A scan for this query is created and returned, as follows.
    * First, the method materializes its LHS and RHS queries.
    * It then determines the optimal chunk size,
    * based on the size of the materialized RHS file and the
    * number of available buffers.
    * It creates a chunk plan for each chunk, saving them in a list.
    * Finally, it creates a multiscan for this list of plans,
    * and returns that scan.
    * @see Plan#open()
    */
   public Scan open() {
      Scan innerscan = inner.open();
      TempTable tt = copyRecordsFrom(outer);
      return new BlockJoinScan(tx, innerscan, tt.tableName(), tt.getLayout(), joinfieldOuter, condOp, joinfieldInner);
   }

   /**
    * Returns an estimate of the number of block accesses
    * required to execute the query. The formula is:
    * <pre> B(product(p1,p2)) = B(p2) + B(p1)*C(p2) </pre>
    * where C(p2) is the number of chunks of p2.
    * The method uses the current number of available buffers
    * to calculate C(p2), and so this value may differ
    * when the query scan is opened.
    * @see Plan#blocksAccessed()
    */
   public int blocksAccessed() {
      // this guesses at the # of chunks
      Plan mpInner = new MaterializePlan(tx, inner); // not opened; just for analysis
      Plan mpOuter = new MaterializePlan(tx, outer); // not opened; just for analysis

      int avail = tx.availableBuffs() - 2;
      int size = mpOuter.blocksAccessed();
      int numchunks = (int)Math.ceil(size * 1.0 / avail);

      int carryoverCost = Math.max(inner.blocksAccessed() +
              outer.blocksAccessed() - mpOuter.blocksAccessed() - mpInner.blocksAccessed(), 0);

      return size + numchunks * mpInner.blocksAccessed() + carryoverCost;
   }

   /**
    * Estimates the number of output records in the product.
    * The formula is:
    * <pre> R(product(p1,p2)) = R(p1)*R(p2) </pre>
    * @see Plan#recordsOutput()
    */
   public int recordsOutput() {
      return inner.recordsOutput() * outer.recordsOutput();
   }

   /**
    * Estimates the distinct number of field values in the product.
    * Since the product does not increase or decrease field values,
    * the estimate is the same as in the appropriate underlying query.
    * @see Plan#distinctValues(String)
    */
   public int distinctValues(String fldname) {
      if (inner.schema().hasField(fldname))
         return inner.distinctValues(fldname);
      else
         return outer.distinctValues(fldname);
   }

   /**
    * Returns the schema of the product,
    * which is the union of the schemas of the underlying queries.
    * @see Plan#schema()
    */
   public Schema schema() {
      return schema;
   }

   private TempTable copyRecordsFrom(Plan p) {
      Scan   src = p.open(); 
      Schema sch = p.schema();
      TempTable t = new TempTable(tx, sch);
      UpdateScan dest = (UpdateScan) t.open();
      while (src.next()) {
         dest.insert();
         for (String fldname : sch.fields())
            dest.setVal(fldname, src.getVal(fldname));
      }
      src.close();
      dest.close();
      return t;
   }

       /**
     * Returns the schema of the index join.
     *
     * @see Plan#getChain()
     */
    public ExecutionChain getChain() {
      return new Join(this, outer.getChain(), inner.getChain(), joinfieldOuter,
              condOp.toString(), joinfieldInner);
   }
}
