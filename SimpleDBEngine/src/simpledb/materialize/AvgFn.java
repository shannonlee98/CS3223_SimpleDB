package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

import java.util.HashSet;
import java.util.Set;

/**
 * The <i>avg</i> aggregation function.
 */
public class AvgFn implements AggregationFn {
   private String fldname;
   private boolean isDistinct;
   private Set<Integer> values = new HashSet<>();
   private int sum;
   private int count;

   /**
    * Create a avg aggregation function for the specified field.
    * @param fldname the name of the aggregated field
    */
   public AvgFn(String fldname, boolean isDistinct) {
      this.fldname = fldname;
      this.isDistinct = isDistinct;
   }
   
   /**
    * Start a new count.
    * Since SimpleDB does not support null values,
    * every record will be counted,
    * regardless of the field.
    * The current sum is thus set to first int val.
    * The current count is set to 1.
    * @see AggregationFn#processFirst(Scan)
    */
   public void processFirst(Scan s) {
      sum = s.getInt(fldname);
      count = 1;
      values.add(sum);
   }
   
   /**
    * Since SimpleDB does not support null values,
    * add the next int value to the sum
    * add one to the count since we assume no null values
    * @see AggregationFn#processNext(Scan)
    */
   public void processNext(Scan s) {
      sum += s.getInt(fldname);
      count ++;
      values.add(s.getInt(fldname));
   }
   
   /**
    * Return the field's name, prepended by "avgof".
    * @see AggregationFn#fieldName()
    */
   public String fieldName() {
      return "avgof" + fldname;
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
    * Return the current avg.
    * @see AggregationFn#value()
    */
   public Constant value() {
      if (isDistinct) {
         sum = 0;
         count = values.size();
         for (int val : values)
            sum += val;
      }
      return new Constant(sum/count);
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
