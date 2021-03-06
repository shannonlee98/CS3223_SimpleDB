package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

import java.util.HashSet;
import java.util.Set;

/**
 * The <i>sum</i> aggregation function.
 */
public class SumFn implements AggregationFn {
   private String fldname;
   private boolean isDistinct;
   private Set<Integer> values = new HashSet<>();
   private int sum;

   /**
    * Create a count aggregation function for the specified field.
    * @param fldname the name of the aggregated field
    */
   public SumFn(String fldname, boolean isDistinct) {
      this.fldname = fldname;
      this.isDistinct = isDistinct;
   }
   
   /**
    * Start a new count.
    * Since SimpleDB does not support null values,
    * every record will be counted,
    * regardless of the field.
    * The current sum is thus set to first int val.
    * @see AggregationFn#processFirst(Scan)
    */
   public void processFirst(Scan s) {
      sum = s.getInt(fldname);
      values.add(sum);
   }
   
   /**
    * Since SimpleDB does not support null values,
    * add the next int value to the sum
    * @see AggregationFn#processNext(Scan)
    */
   public void processNext(Scan s) {
      sum += s.getInt(fldname);
      values.add(s.getInt(fldname));
   }
   
   /**
    * Return the field's name, prepended by "sumof".
    * @see AggregationFn#fieldName()
    */
   public String fieldName() {
      return "sumof" + fldname;
   }

   /**
    * Return the field name to be aggregated on.
    *
    * @return the field name to be aggregated on
    */
   public String field() {
      return fldname;
   }

   /**
    * Return the current sum.
    * @see AggregationFn#value()
    */
   public Constant value() {
      if (isDistinct) {
         sum = 0;
         for (Integer val : values)
            sum += val;
      }
      return new Constant(sum);
   }

   /**
    * Return if the aggregated value is always an integer.
    *
    * @return if the aggregated value is always an integer
    */
   public boolean isAlwaysInteger() {
      return true;
   }
}
