package simpledb.multibuffer;

import simpledb.query.CondOp;
import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.query.Term;
import simpledb.record.Layout;
import simpledb.tx.Transaction;

/**
 * The Scan class for the <i>blockjoin</i> operator.
 */
public class BlockJoinScan implements Scan {
   private Transaction tx;
   private Scan inner, outer =null;
   private String filename, joinfieldOuter, joinfieldInner;
   private Layout layout;
   private int chunksize, nextblknum, filesize;
   private CondOp condOp;
   private boolean isEmpty;


   /**
    * Creates a blockjoin scan class for the innerscan and the scan of the temp table.
    * @param tx the current transaction
    * @param innerscan the inner scan
    * @param tblname the temp table name with outer scan
    * @param layout the metadata for the outer scan's table
    * @param joinfieldOuter the joinfield of the outer table
    * @param condOp the conditional operator between the joinfields
    * @param joinfieldInner the joinfield of the inner table
    */
   public BlockJoinScan(Transaction tx, Scan innerscan, String tblname, Layout layout,
                        String joinfieldOuter, CondOp condOp, String joinfieldInner) {
      this.condOp = condOp;
      this.tx = tx;
      this.inner = innerscan;
      this.filename = tblname + ".tbl";
      this.layout = layout;
      filesize = tx.size(filename);
      int available = Math.min(1, tx.availableBuffs() - 2);
      chunksize = BufferNeeds.bestFactor(available, filesize);
      beforeFirst();
      this.joinfieldOuter = joinfieldOuter;
      this.joinfieldInner = joinfieldInner;
   }
   
   /**
    * Positions the scan before the first record.
    * That is, the inner scan is positioned at its first record,
    * and the outer scan is positioned before the first record of the first chunk.
    * @see Scan#beforeFirst()
    */
   public void beforeFirst() {
      nextblknum = 0;
      useNextChunk();
      inner.beforeFirst();
      isEmpty = !inner.next();
   }
   
   /**
    * Moves to the next record in the current scan.
    * If there are no more records in the current chunk,
    * then move to the next inner record and the beginning of that chunk.
    * If there are no more outer-chunk records, then move to the next chunk
    * and begin again.
    * @see Scan#next()
    */
   public boolean next() {
      if (isEmpty) {
         return false;
      }

      while(true){
         //no matter what we advance outer.
         boolean outerhasMore = outer.next();
         if (!outerhasMore) {
            //if outer has no more, we can go on to the next inner. It's either time to reset outer, or go next chunk
            boolean innerhasMore = inner.next();
            //if outer has no more and inner has no more, time to go next chunk
            if (!innerhasMore) {
               //if no more next chunk, we are done.
               if (!useNextChunk()) {
                  return false;
               }
               //if there is still a next chunk, reset both inner and outer

               //check if next chunk is empty
               if (!outer.next()) return false;

               inner.beforeFirst();

               //check if inner is empty
               if (!inner.next()) return false;
            } else {
            //outer has no more but inner has more, reset outer
               outer.beforeFirst();
               if (!outer.next()) return false;
            }
         }

         if (condOp.evaluate(outer.getVal(joinfieldOuter), inner.getVal(joinfieldInner))) {
            return true;
         }
      }
   }
   
   /**
    * Closes the current scans.
    * @see Scan#close()
    */
   public void close() {
      inner.close();
      outer.close();
   }
   
   /** 
    * Returns the value of the specified field.
    * The value is obtained from whichever scan
    * contains the field.
    * @see Scan#getVal(String)
    */
   public Constant getVal(String fldname) {
      if (inner.hasField(fldname))
         return inner.getVal(fldname);
      else
         return outer.getVal(fldname);
   }
   
   /** 
    * Returns the integer value of the specified field.
    * The value is obtained from whichever scan
    * contains the field.
    * @see Scan#getInt(String)
    */
   public int getInt(String fldname) {
      if (inner.hasField(fldname))
         return inner.getInt(fldname);
      else
         return outer.getInt(fldname);
   }
   
   /** 
    * Returns the string value of the specified field.
    * The value is obtained from whichever scan
    * contains the field.
    * @see Scan#getString(String)
    */
   public String getString(String fldname) {
      if (inner.hasField(fldname))
         return inner.getString(fldname);
      else
         return outer.getString(fldname);
   }
   
   /**
    * Returns true if the specified field is in
    * either of the underlying scans.
    * @see Scan#hasField(String)
    */
   public boolean hasField(String fldname) {
      return inner.hasField(fldname) || outer.hasField(fldname);
   }
   
   private boolean useNextChunk() {
      if (nextblknum >= filesize)
         return false;
      if (outer != null)
         outer.close();
      int end = nextblknum + chunksize - 1;
      if (end >= filesize)
         end = filesize - 1;
      outer = new ChunkScan(tx, filename, layout, nextblknum, end);
      outer.beforeFirst();
      nextblknum = end + 1;
      return true;
   }
}

