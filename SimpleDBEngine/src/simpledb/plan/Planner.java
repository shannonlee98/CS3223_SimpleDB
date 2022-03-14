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
 
   // SimpleDB does not verify queries, although it should.
   private void verifyQuery(QueryData data, Transaction tx) {
      Collection<String> tables = data.tables();
      Schema schema = new Schema();
      for (String tblname : tables) {
         schema.addAll(qplanner.getSchema(tblname, tx));
      }

      LinkedHashMap<String, Boolean> orderFields = data.orderByFields();
      verifyFields(orderFields.keySet(), schema);
      verifyFields(data.groupByFields(), schema);
      verifyFields(data.aggregatesFields(), schema);
      verifySelectFields(data.fields(), schema, data.aggregates());

      Predicate pred = data.pred();
      verifyFields(pred.getFields(), schema);
   }

   private void verifyFields(Collection<String> fields, Schema schema) {
      for (String fldname : fields)
         if (!schema.hasField(fldname))
            throw new BadSyntaxException("Field '" + fldname + "' does not exist");
   }

   private void verifySelectFields(Collection<String> fields, Schema schema, List<AggregationFn> aggregates){
      for (String fldname : fields)
         if (!schema.hasField(fldname))
            for (AggregationFn aggrFn : aggregates)
               if (!fldname.equals(aggrFn.fieldName()))
                  throw new BadSyntaxException("Field '" + fldname + "' does not exist");
   }

   // SimpleDB does not verify updates, although it should.
   private void verifyUpdate(Object data) {
   }
}
