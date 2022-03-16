package simpledb.opt;

import java.util.*;

import simpledb.display.Display;
import simpledb.materialize.DistinctPlan;
import simpledb.materialize.GroupByPlan;
import simpledb.materialize.SortPlan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;
import simpledb.metadata.MetadataMgr;
import simpledb.parse.QueryData;
import simpledb.plan.*;

/**
 * A query planner that optimizes using a heuristic-based algorithm.
 * @author Edward Sciore
 */
public class HeuristicQueryPlanner implements QueryPlanner {
   private Collection<TablePlanner> tableplanners = new ArrayList<>();
   private MetadataMgr mdm;
   
   public HeuristicQueryPlanner(MetadataMgr mdm) {
      this.mdm = mdm;
   }
   
   /**
    * Creates an optimized left-deep query plan using the following
    * heuristics.
    * H1. Choose the smallest table (considering selection predicates)
    * to be first in the join order.
    * H2. Add the table to the join order which
    * results in the smallest output.
    */
   public Plan createPlan(QueryData data, Transaction tx) {

      // Step 1:  Create a TablePlanner object for each mentioned table
      for (String tblname : data.tables()) {
         TablePlanner tp = new TablePlanner(tblname, data.pred(), tx, mdm, data.isDistinct());
         tableplanners.add(tp);
      }

      // Step 2:  Choose the lowest-size plan to begin the join order
      Plan currentplan = getLowestSelectPlan();

      // Step 3:  Repeatedly add a plan to the join order
      while (!tableplanners.isEmpty()) {
         Plan p = getLowestJoinPlan(currentplan);
         if (p != null) {
            currentplan = p;
         }
         else  // no applicable join
            currentplan = getLowestProductPlan(currentplan);
      }

      // Step 4: Add a sort plan if ordered
      if (!data.orderByFields().isEmpty()) {
         currentplan = new SortPlan(tx, currentplan, data.orderByFields(), data.isDistinct());
      }

      // Step 5: Add a group plan if there is aggregation or 'group by'
      if (!data.groupByFields().isEmpty() || !data.aggregates().isEmpty()) {
         currentplan = new GroupByPlan(tx, currentplan, data.groupByFields(), data.aggregates(), data.isDistinct());
      }

      // Step 6:  Project on the field names and return
      currentplan = new ProjectPlan(currentplan, data.fields());

      // Step 7: Add a distinct plan if isDistinct is true
      if (data.isDistinct()) {
         currentplan = new DistinctPlan(tx, currentplan, data.fields());
      }

      Display.getInstance().print(currentplan.GetEC());
      return currentplan;
   }
   
   private Plan getLowestSelectPlan() {
      TablePlanner besttp = null;
      Plan bestplan = null;
      for (TablePlanner tp : tableplanners) {
         Plan plan = tp.makeSelectPlan();
         if (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput()) {
            besttp = tp;
            bestplan = plan;
         }
      }
      tableplanners.remove(besttp);
      return bestplan;
   }
   
   private Plan getLowestJoinPlan(Plan current) {
      TablePlanner besttp = null;
      Plan bestplan = null;
      for (TablePlanner tp : tableplanners) {
         Plan plan = tp.makeJoinPlan(current);
         if (plan != null && (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput())) {
            besttp = tp;
            bestplan = plan;
         }
      }
      if (bestplan != null)
         tableplanners.remove(besttp);
      return bestplan;
   }
   
   private Plan getLowestProductPlan(Plan current) {
      TablePlanner besttp = null;
      Plan bestplan = null;
      for (TablePlanner tp : tableplanners) {
         Plan plan = tp.makeProductPlan(current);
         if (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput()) {
            besttp = tp;
            bestplan = plan;
         }
      }
      tableplanners.remove(besttp);
      return bestplan;
   }

   public void setPlanner(Planner p) {
      // for use in planning views, which
      // for simplicity this code doesn't do.
   }

   /**
    * Returns the schema of the specified table
    * @param tblname the table name
    * @param tx the calling transaction
    * @return schema of the specified table
    */
   public Schema getSchema(String tblname, Transaction tx) {
      return mdm.getSchema(tblname, tx);
   }
}
