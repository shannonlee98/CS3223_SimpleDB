package simpledb.hash;

import simpledb.materialize.TempTable;
import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

/**
 * The Plan class for the <i>HashPartitionPlan</i> operator.
 * @author Edward Sciore
 */
public class HashPartitionPlan implements Plan {
   private Plan srcplan;
   private Transaction tx;
   private int partitions;
   private String hashonfield;
   private int hash;

   /**
    * Create a hash partition plan for the specified query.
    * @param srcplan the plan of the underlying query
    * @param tx the calling transaction
    */
   public HashPartitionPlan(Transaction tx, Plan srcplan, int partitions,
                            String hashonfield, int hash) {
      this.srcplan = srcplan;
      this.tx = tx;
      this.partitions = partitions;
      this.hashonfield = hashonfield;
      this.hash = hash;
   }
   
   /**
    * This method loops through the underlying query,
    * copying its output records into a temporary table.
    * It then returns a table scan for that table.
    * @see Plan#open()
    */
   public Scan open() {
      Scan src = srcplan.open();
      return new HashPartitionScan(src, hash, partitions, hashonfield);
   }
   
   /**
    * Return the estimated number of blocks in the 
    * materialized table.
    * It does <i>not</i> include the one-time cost
    * of materializing the records.
    * @see Plan#blocksAccessed()
    */
   public int blocksAccessed() {
      return srcplan.blocksAccessed();
   }
   
   /**
    * Return the number of records in the materialized table,
    * which is the same as in the underlying plan.
    * @see Plan#recordsOutput()
    */
   public int recordsOutput() {
      return (int)Math.ceil(srcplan.recordsOutput() * 1.0 / partitions);
   }
   
   /**
    * Return the number of distinct field values,
    * which is the same as in the underlying plan.
    * @see Plan#distinctValues(String)
    */
   public int distinctValues(String fldname) {
      return (int)Math.ceil(srcplan.distinctValues(fldname) * 1.0 / partitions);
   }
   
   /**
    * Return the schema of the materialized table,
    * which is the same as in the underlying plan.
    * @see Plan#schema()
    */
   public Schema schema() {
      return srcplan.schema();
   }
}
