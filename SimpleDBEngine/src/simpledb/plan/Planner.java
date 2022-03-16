package simpledb.plan;

import simpledb.materialize.AggregationFn;
import simpledb.query.Predicate;
import simpledb.record.Schema;
import simpledb.tx.Transaction;
import simpledb.parse.*;

import java.util.*;

/**
 * The object that executes SQL statements.
 * @author Edward Sciore
 */
public class Planner {
   private QueryPlanner qplanner;
   private UpdatePlanner uplanner;

   public Planner(QueryPlanner qplanner, UpdatePlanner uplanner) {
      this.qplanner = qplanner;
      this.uplanner = uplanner;
   }
   
   /**
    * Creates a plan for an SQL select statement, using the supplied planner.
    * @param qry the SQL query string
    * @param tx the transaction
    * @return the scan corresponding to the query plan
    */
   public Plan createQueryPlan(String qry, Transaction tx) {
      Parser parser = new Parser(qry);
      QueryData data = parser.query();
      verifyQuery(data, tx);
      return qplanner.createPlan(data, tx);
   }

   /**
    * Execute a setting.
    */
   public void executeSetting(String qry) {
      new Parser(qry).setting();
   }
   
   /**
    * Executes an SQL insert, delete, modify, or
    * create statement.
    * The method dispatches to the appropriate method of the
    * supplied update planner,
    * depending on what the parser returns.
    * @param cmd the SQL update string
    * @param tx the transaction
    * @return an integer denoting the number of affected records
    */
   public int executeUpdate(String cmd, Transaction tx) {
      Parser parser = new Parser(cmd);
      Object data = parser.updateCmd();
      verifyUpdate(data);
      if (data instanceof InsertData)
         return uplanner.executeInsert((InsertData)data, tx);
      else if (data instanceof DeleteData)
         return uplanner.executeDelete((DeleteData)data, tx);
      else if (data instanceof ModifyData)
         return uplanner.executeModify((ModifyData)data, tx);
      else if (data instanceof CreateTableData)
         return uplanner.executeCreateTable((CreateTableData)data, tx);
      else if (data instanceof CreateViewData)
         return uplanner.executeCreateView((CreateViewData)data, tx);
      else if (data instanceof CreateIndexData)
         return uplanner.executeCreateIndex((CreateIndexData)data, tx);
      else
         return 0;
   }

   /**
    * Checks if the entire query is valid. Gets schema from all
    * tables selected and checks if the fields listed in the query
    * are in the schema
    * @param data data of the query
    * @param tx the transaction
    */
   private void verifyQuery(QueryData data, Transaction tx) {
      Collection<String> tables = data.tables();
      if (tables.size() == 0)
         throw new BadSyntaxException("Table list is empty");
      Schema schema = new Schema();
      for (String tblname : tables) {
         schema.addAll(qplanner.getSchema(tblname, tx));
      }

      LinkedHashMap<String, Boolean> orderFields = data.orderByFields();
      verifyFields(orderFields.keySet(), schema);
      verifyFields(data.groupByFields(), schema);
      verifyFields(data.aggregatesFields(), schema);
      verifySelectFields(data, schema);

      Predicate pred = data.pred();
      verifyFields(pred.getFields(), schema);
   }

   /**
    * Checks if each field in the collection are in the schema,
    * throws error if it is not found
    * @param fields Collection of fields to be verified
    * @param schema Schema of the all fields from selected tables
    */
   private void verifyFields(Collection<String> fields, Schema schema) {
      for (String fldname : fields)
         if (!schema.hasField(fldname) && !fldname.equals("*"))
            throw new BadSyntaxException("Field '" + fldname + "' does not exist");
   }

   /**
    * Checks if each field is in the schema or in the aggregation
    * functions. Throws error if the field is one that is already
    * used in the aggregation function. Throws error if the field
    * is not in the schema and also not the column name of any
    * aggregation function
    * @param data data of the query
    * @param schema Schema of the all fields from selected tables
    */
   private void verifySelectFields(QueryData data, Schema schema){
      for (String fldname : data.fields()) {
         if (fldname.equals("*"))
            continue;
         if (isAggregationFunction(data.aggregates(), fldname))
            continue;
         if (!schema.hasField(fldname)) {
            throw new BadSyntaxException("Field '" + fldname + "' does not exist");
         }
         if (!data.aggregates().isEmpty() && !hasGroupField(data, fldname))
            throw new BadSyntaxException("Field '" + fldname + "' should be included in group by clause");
      }
   }

   private boolean hasGroupField(QueryData data, String fldname) {
      for (String group : data.groupByFields())
         if (group.equals(fldname))
            return true;
      return false;
   }

   /**
    * Checks if the field name is already used in any
    * aggregation function
    * @param aggregates List of aggregate functions
    * @param fldname Field name to be verified
    * @return True if the field is found in the aggregation
    * function list, false otherwise
    */
   private boolean isAggregationField(List<AggregationFn> aggregates, String fldname) {
      for (AggregationFn aggrFn : aggregates)
         if (fldname.equals(aggrFn.field()))
            return true;
      return false;
   }

   /**
    * Checks if the field name is a column name of any
    * aggregation functions
    * @param aggregates List of aggregate functions
    * @param fldname Field name to be verified
    * @return True if the field is a column name of any
    * aggregation function, false otherwise
    */
   private boolean isAggregationFunction(List<AggregationFn> aggregates, String fldname) {
      for (AggregationFn aggrFn : aggregates)
         if (fldname.equals(aggrFn.fieldName()))
            return true;
      return false;
   }

   // SimpleDB does not verify updates, although it should.
   private void verifyUpdate(Object data) {
   }
}
