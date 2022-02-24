package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.record.RID;
import simpledb.record.TableScan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.util.List;

/**
 * The scan class corresponding to the indexjoin relational
 * algebra operator.
 * The code is very similar to that of ProductScan,
 * which makes sense because an index join is essentially
 * the product of each LHS record with the matching RHS index records.
 *
 * @author Edward Sciore
 */
public class BlockJoinScan implements Scan {
   private String joinfieldInner;
   private String joinfieldOuter;

   //blockJoin Vars
   private int blockSize;
   private TableScan outer; //we will compute this in the constructor and do away with lhs n rhs scan.
   private Scan inner;
   private RID ridOfBeforeFirstBlockRecord;
   private int currentRecordOfBlock;


   /**
    * Creates a block join scan for the specified LHS scan and
    * RHS scan.
    *
    * @param tx             the current transaction
    * @param outer          the outer scan
    * @param joinfieldOuter the outer field used for joining
    * @param inner          the inner scan
    * @param joinfieldInner the inner field used for joining
    */
   public BlockJoinScan(Transaction tx, TableScan outer, String joinfieldOuter, Scan inner, String joinfieldInner) {
      this.joinfieldInner = joinfieldInner;
      this.joinfieldOuter = joinfieldOuter;
      this.blockSize = Math.max(tx.availableBuffs() - 2, 1);
      this.outer = outer;
      this.inner = inner;

      /**for loop to copy a block size of (blockSize) into an array [block] with next();
       * <- the hope is less I/O since we are doing consecutive access on disk.
       * we might need to flip lhs or rhs to be the one that is stored in blockBuffer
       * (inner/outer) based on a heuristic.
       * for now, we fix as lhs being the outer (one which goes through once)
       *
       * Need to find out how many constants can fit in a block of
       */

      beforeFirst();
   }

   /**
    * Positions the scan before the first record.
    * That is, the LHS scan will be positioned at its
    * first record, and the RHS will be positioned
    * before the first record for the join value.
    *
    * @see Scan#beforeFirst()
    */
   public void beforeFirst() {
      outer.beforeFirst();
      ridOfBeforeFirstBlockRecord = outer.getRid();
      inner.beforeFirst();
      currentRecordOfBlock = -1;
   }

   /**
    * Moves the scan to the next record.
    * The method moves to the next RHS record, if possible.
    * Otherwise, it moves to the next LHS record and the
    * first RHS record.
    * If there are no more LHS records, the method returns false.
    *
    * @see Scan#next()
    */

   //there is a point that my outer is suppose to go next block but it repeats the current block instead.
   public boolean next() { //this next is like the iterator of this whole thing

      //should only call inner.next() after outer.next() is done.
      //if you terminate outer.next() early, do not call inner.next() and do not reset outer.next().

      boolean outerHasMore = outer.next();
      currentRecordOfBlock = (currentRecordOfBlock + 1) % blockSize;
      boolean innerHasMore;

      while (true) {

         if (currentRecordOfBlock == 0 || !outerHasMore) {
            //if about to start a block, call inner.next
            //if just completed below for loop, it can only enter here.
            innerHasMore = inner.next();

            if (!innerHasMore && !outerHasMore) {
               return false;
            } else if (!innerHasMore) {
               //next block
               inner.beforeFirst();
               inner.next();

               //travel to next block of outer
               outer.moveToRid(ridOfBeforeFirstBlockRecord);
               for (; currentRecordOfBlock < blockSize; currentRecordOfBlock++) {
                  if (!outer.next()) return false;
               }
               ridOfBeforeFirstBlockRecord = outer.getRid();
               currentRecordOfBlock = 0;
               outer.next();
            } else {
               //reset if inner().next is called.
               outer.moveToRid(ridOfBeforeFirstBlockRecord);
               currentRecordOfBlock = 0;
               outer.next();
            }
         }

         for (; currentRecordOfBlock < blockSize; currentRecordOfBlock++) {
//            System.out.println("Comparing " +
//                    outer.getVal(joinfieldOuter) + " with " +
//                    inner.getVal(joinfieldInner));
            if (outer.getVal(joinfieldOuter).equals(inner.getVal(joinfieldInner))) {
               return true;
            }

            outerHasMore = outer.next();
            if (!outerHasMore) {
               break;
            }
         } //if break from this loop, either (outer value must reset + inner.next) or end.
         currentRecordOfBlock = 0;
      }
   }

   /**
    * Returns the integer value of the specified field.
    *
    * @see Scan#getVal(String)
    */
   public int getInt(String fldname) {
      if (inner.hasField(fldname))
         return inner.getInt(fldname);
      else
         return outer.getInt(fldname);
   }

   /**
    * Returns the Constant value of the specified field.
    *
    * @see Scan#getVal(String)
    */
   public Constant getVal(String fldname) {
      if (inner.hasField(fldname))
         return inner.getVal(fldname);
      else
         return outer.getVal(fldname);
   }

   /**
    * Returns the string value of the specified field.
    *
    * @see Scan#getVal(String)
    */
   public String getString(String fldname) {
      if (inner.hasField(fldname))
         return inner.getString(fldname);
      else
         return outer.getString(fldname);
   }

   /**
    * Returns true if the field is in the schema.
    *
    * @see Scan#hasField(String)
    */
   public boolean hasField(String fldname) {
      return inner.hasField(fldname) || outer.hasField(fldname);
   }

   /**
    * Closes the scan by closing its LHS scan and its RHS index.
    *
    * @see Scan#close()
    */
   public void close() {
      inner.close();
      outer.close();
   }
}
