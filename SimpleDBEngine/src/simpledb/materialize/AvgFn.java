package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

/**
 * The <i>avg</i> aggregation function.
 */
public class AvgFn implements AggregationFn {
   private String fldname;
   private int sum;
   private int count;

   /**
    * Create a avg aggregation function for the specified field.
    * @param fldname the name of the aggregated field
    */
   public AvgFn(String fldname) {
      this.fldname = fldname;
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
