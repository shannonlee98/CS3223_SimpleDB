package simpledb.parse;

import java.util.*;

import simpledb.query.*;

/**
 * Data for the SQL <i>select</i> statement.
 * @author Edward Sciore
 */
public class QueryData {
   private List<String> selectFields;
   private Collection<String> tables;
   private Predicate pred;
   private Map<String, Boolean> orderByFields;


   /**
    * Saves the field and table list and predicate.
    */
   public QueryData(List<String> selectFields, Collection<String> tables, Predicate pred, Map<String, Boolean> orderByFields) {
      this.selectFields = selectFields;
      this.tables = tables;
      this.pred = pred;
      this.orderByFields = orderByFields;
   }
   
   /**
    * Returns the fields mentioned in the select clause.
    * @return a list of field names
    */
   public List<String> selectFields() {
      return selectFields;
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

   public Map<String, Boolean> OrderByFields() {
      return orderByFields;
   }
   
   public String toString() {
      String result = "select ";
      for (String fldname : selectFields)
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
