package simpledb.parse;

import java.util.*;

import simpledb.query.*;
import simpledb.materialize.AggregationFn;

/**
 * Data for the SQL <i>select</i> statement.
 * @author Edward Sciore
 */
public class QueryData {
   private List<String> fields;
   private List<AggregationFn> aggregates;
   private Collection<String> tables;
   private Predicate pred;
   private Map<String, Boolean> orderByFields;
   private List<String> groupByFields;


   /**
    * Saves the field and table list and predicate.
    */
   public QueryData(List<String> fields, List<AggregationFn> aggregates, Collection<String> tables, Predicate pred,
                    Map<String, Boolean> orderByFields, List<String> groupByFields) {
      this.fields = fields;
      this.aggregates = aggregates;
      this.tables = tables;
      this.pred = pred;
      this.orderByFields = orderByFields;
      this.groupByFields = groupByFields;
      System.out.println(Arrays.toString(fields.toArray()));
      System.out.println(Arrays.toString(aggregates.toArray()));
      System.out.println(Arrays.toString(groupByFields.toArray()));
   }
   
   /**
    * Returns the fields mentioned in the select clause.
    * @return a list of field names
    */
   public List<String> fields() {
      return fields;
   }

   /**
    * Returns the aggregates mentioned in the select clause.
    * @return a list of pairs containing aggregate and field names
    */
   public List<AggregationFn> aggregates() {return aggregates; }

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

   public Map<String, Boolean> orderByFields() {
      return orderByFields;
   }

   public List<String> groupByFields() { return groupByFields; }

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
