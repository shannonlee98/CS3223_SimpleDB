package simpledb.hash;

import simpledb.materialize.TempTable;
import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

import java.util.*;

/**
 * The Plan class corresponding to the <i>indexjoin</i>
 * relational algebra operator.
 *
 * @author Edward Sciore
 */
public class HashJoinPlan implements Plan {
   private Plan p1, p2;
   private Transaction tx;
   private String joinfield1;
   private String joinfield2;
   private Schema sch = new Schema();
   private int partitions;

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
      this.p1 = p1;
      this.p2 = p2;

      this.joinfield1 = joinfield1;
      this.joinfield2 = joinfield2;

      partitions = Math.max(tx.availableBuffs() - 1, 1);

      sch.addAll(p1.schema());
      sch.addAll(p2.schema());
      this.tx = tx;
   }

   /**
    * Opens an indexjoin scan for this query
    *
    * @see Plan#open()
    */
   public Scan open() {
      ArrayList<Plan> partitions1 = partition(p1, joinfield1);
      ArrayList<Plan> partitions2 = partition(p2, joinfield2);
      //join the values by hashtable.
      return new HashJoinScan(partitions1, joinfield1, partitions2,
              joinfield2, partitions, p1.schema().fields());
   }

   private ArrayList<Plan> partition(Plan p, String joinfield) {
      ArrayList<Plan> partitionList = new ArrayList<>();

      for (int i = 0; i < partitions; i ++) {
         partitionList.add(new HashPartitionPlan(tx, p, partitions, joinfield, i));
      }

      return partitionList;
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
      //include the partition cost also
      return p1.blocksAccessed() * 3 + p2.blocksAccessed() * 3;
   }

   /**
    * Estimates the number of output records in the join.
    * The formula is:
    * <pre> R(indexjoin(p1,p2,idx)) = R(p1)*R(idx) </pre>
    *
    * @see Plan#recordsOutput()
    */
   public int recordsOutput() {
      return p1.recordsOutput() * p2.recordsOutput();
   }

   /**
    * Estimates the number of distinct values for the
    * specified field.
    *
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
    *
    * @see Plan#schema()
    */
   public Schema schema() {
      return sch;
   }
}