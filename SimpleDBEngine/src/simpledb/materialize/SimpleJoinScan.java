package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

/**
 * The scan class corresponding to the indexjoin relational
 * algebra operator.
 * The code is very similar to that of ProductScan, 
 * which makes sense because an index join is essentially
 * the product of each LHS record with the matching RHS index records.
 * @author Edward Sciore
 */
public class SimpleJoinScan implements Scan {
   private Scan lhs;
   private Transaction tx;
   private String joinfield1;
   private String joinfield2;
   private TableScan rhs;

   /**
    * Creates a block join scan for the specified LHS scan and
    * RHS scan.
    * @param tx the current transaction
    * @param lhs the LHS scan
    * @param joinfield1 the LHS field used for joining
    * @param joinfield2 the LHS field used for joining
    * @param rhs the RHS scan
    */
   public SimpleJoinScan(Transaction tx, Scan lhs, String joinfield1, TableScan rhs, String joinfield2) {
      this.lhs = lhs;
      this.tx = tx;
      this.joinfield1 = joinfield1;
      this.rhs = rhs;
      this.joinfield2 = joinfield2;

      beforeFirst();
   }
   
   /**
    * Positions the scan before the first record.
    * That is, the LHS scan will be positioned at its
    * first record, and the RHS will be positioned
    * before the first record for the join value.
    * @see Scan#beforeFirst()
    */
   public void beforeFirst() {
      lhs.beforeFirst();
      lhs.next();
   }

   /**
    * Moves the scan to the next record.
    * The method moves to the next RHS record, if possible.
    * Otherwise, it moves to the next LHS record and the
    * first RHS record.
    * If there are no more LHS records, the method returns false.
    * @see Scan#next()
    */
   public boolean next() {
      while (true) {
         while (rhs.next()) {
            if (lhs.getVal(joinfield1).equals(rhs.getVal(joinfield2))) {
               return true;
            }
         }
         rhs.beforeFirst();
         if (!lhs.next())
            return false;
      }
   }
   
   /**
    * Returns the integer value of the specified field.
    * @see Scan#getVal(String)
    */
   public int getInt(String fldname) {
      if (rhs.hasField(fldname))
         return rhs.getInt(fldname);
      else  
         return lhs.getInt(fldname);
   }
   
   /**
    * Returns the Constant value of the specified field.
    * @see Scan#getVal(String)
    */
   public Constant getVal(String fldname) {
      if (rhs.hasField(fldname))
         return rhs.getVal(fldname);
      else
         return lhs.getVal(fldname);
   }
   
   /**
    * Returns the string value of the specified field.
    * @see Scan#getVal(String)
    */
   public String getString(String fldname) {
      if (rhs.hasField(fldname))
         return rhs.getString(fldname);
      else
         return lhs.getString(fldname);
   }
   
   /** Returns true if the field is in the schema.
     * @see Scan#hasField(String)
     */
   public boolean hasField(String fldname) {
      return rhs.hasField(fldname) || lhs.hasField(fldname);
   }
   
   /**
    * Closes the scan by closing its LHS scan and its RHS index.
    * @see Scan#close()
    */
   public void close() {
      lhs.close();
      rhs.close();
   }
}
