package simpledb.parse;

import java.util.*;

import simpledb.query.*;
import simpledb.materialize.AggregationFn;

/**
 * Data for the SQL <i>select</i> statement.
 * @author Edward Sciore
 */
public class QueryData {
   private boolean isDistinct;
   private List<String> fields;
   private List<AggregationFn> aggregates;
   private Collection<String> tables;
   private Predicate pred;
   private LinkedHashMap<String, Boolean> orderByFields;
   private List<String> groupByFields;


   /**
    * Saves the field and table list and predicate.
    */
   public QueryData(boolean isDistinct, List<String> fields, List<AggregationFn> aggregates, Collection<String> tables, Predicate pred,
                    LinkedHashMap<String, Boolean> orderByFields, List<String> groupByFields) {
      this.isDistinct = isDistinct;
      this.fields = fields;
      this.aggregates = aggregates;
      this.tables = tables;
      this.pred = pred;
      this.orderByFields = orderByFields;
      this.groupByFields = groupByFields;
   }

   /**
    * Returns whether the query has distinct.
    * @return true if query has the distinct keyword, false otherwise
    */
   public boolean isDistinct() { return isDistinct; }
   
   /**
    * Returns the fields mentioned in the select clause.
    * @return a list of field names
    */
   public List<String> fields() {
      return fields;
   }

   /**
    * Returns the aggregates mentioned in the select clause.
    * @return a list of aggregates
    */
   public List<AggregationFn> aggregates() { return aggregates; }

   /**
    * Returns the fields that are aggregated on
    * @return a list of fields that are agrregated on
    */
   public List<String> aggregatesFields() {
      List<String> aggregateFields = new ArrayList<>();
      for (AggregationFn aggrFn : aggregates)
         aggregateFields.add(aggrFn.field());
      return aggregateFields;
   }

   /**
    * Checks if the field name is a column name of any
    * aggregation functions
    * @param fldname Field name to be verified
    * @return True if the field is a column name of any
    * aggregation function, false otherwise
    */
   public boolean isAggregationFunction(String fldname) {
      for (AggregationFn aggrFn : aggregates)
         if (fldname.equals(aggrFn.fieldName()))
            return true;
      return false;
   }

   /**
    * Returns the tables mentioned in the from clause.
    * @return a collection of table names
    */
   public Collection<String> tables() {
      return tables;
   }
   
   /**
    * Returns the predicate that describes which
    * records should be in the output table.
    * @return the query predicate
    */
   public Predicate pred() {
      return pred;
   }

   /**
    * Returns a Linked hash map with field as key and
    * isAsc boolean as value
    * @return the order by fields
    */
   public LinkedHashMap<String, Boolean> orderByFields() {
      return orderByFields;
   }

   /**
    * Returns the list of group by fields
    * @return the group by fields
    */
   public List<String> groupByFields() { return groupByFields; }


   /**
    * Checks if the field provided is in the list of group
    * by fields
    * @param fldname field name to be checked
    * @return true if the field is in the group by field list
    */
   public boolean hasGroupField(String fldname) {
      for (String group : groupByFields)
         if (group.equals(fldname))
            return true;
      return false;
   }

   public String toString() {
      String result = "select ";
      for (String fldname : fields)
         result += fldname + ", ";
      result = result.substring(0, result.length()-2); //remove final comma
      result += " from ";
      for (String tblname : tables)
         result += tblname + ", ";
      result = result.substring(0, result.length()-2); //remove final comma
      String predstring = pred.toString();
      if (!predstring.equals(""))
         result += " where " + predstring;
      return result;
   }
}
