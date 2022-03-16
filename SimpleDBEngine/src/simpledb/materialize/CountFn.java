package simpledb.materialize;

import simpledb.query.*;

import java.util.HashSet;
import java.util.Set;

/**
 * The <i>count</i> aggregation function.
 * @author Edward Sciore
 */
public class CountFn implements AggregationFn {
   private String fldname;
   private boolean isDistinct;
   private Set<Constant> values = new HashSet<>();
   private int count;
   
   /**
    * Create a count aggregation function for the specified field.
    * @param fldname the name of the aggregated field
    */
   public CountFn(String fldname, boolean isDistinct) {
      this.fldname = fldname;
      this.isDistinct = isDistinct;
   }
   
   /**
    * Start a new count.
    * Since SimpleDB does not support null values,
    * every record will be counted,
    * regardless of the field.
    * The current count is thus set to 1.
    * @see simpledb.materialize.AggregationFn#processFirst(simpledb.query.Scan)
    */
   public void processFirst(Scan s) {
      count = 1;
      if (!fldname.equals("*"))
         values.add(s.getVal(fldname));
   }
   
   /**
    * Since SimpleDB does not support null values,
    * this method always increments the count,
    * regardless of the field.
    * @see simpledb.materialize.AggregationFn#processNext(simpledb.query.Scan)
    */
   public void processNext(Scan s) {
      count++;
      if (!fldname.equals("*"))
         values.add(s.getVal(fldname));
   }
   
   /**
    * Return the field's name, prepended by "countof".
    * @see simpledb.materialize.AggregationFn#fieldName()
    */
   public String fieldName() {
      return "countof" + fldname;
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
    * Return the current count.
    * @see simpledb.materialize.AggregationFn#value()
    */
   public Constant value() {
      return isDistinct ? new Constant(values.size()) : new Constant(count);
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
