package simpledb.opt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import simpledb.controller.Setting;
import simpledb.display.ExecutionPath;
import simpledb.display.Join;
import simpledb.hash.GraceHashJoinPlan;
import simpledb.multibuffer.BlockJoinPlan;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.metadata.*;
import simpledb.index.planner.*;
import simpledb.materialize.MergeJoinPlan;
import simpledb.multibuffer.MultibufferProductPlan;
import simpledb.plan.*;

/**
 * This class contains methods for planning a single table.
 *
 * @author Edward Sciore
 */
class TablePlanner {
    private TablePlan myplan;
    private Predicate mypred;
    private Schema myschema;
    private Map<String, IndexInfo> indexes;
    private Transaction tx;
    private boolean isDistinct;

    /**
     * Creates a new table planner.
     * The specified predicate applies to the entire query.
     * The table planner is responsible for determining
     * which portion of the predicate is useful to the table,
     * and when indexes are useful.
     *
     * @param tblname the name of the table
     * @param mypred  the query predicate
     * @param tx      the calling transaction
     */
    public TablePlanner(String tblname, Predicate mypred, Transaction tx, MetadataMgr mdm, boolean isDistinct) {
        this.mypred = mypred;
        this.tx = tx;
        myplan = new TablePlan(tx, tblname, mdm);
        myschema = myplan.schema();
        indexes = mdm.getIndexInfo(tblname, tx);
        this.isDistinct = isDistinct;
    }

    /**
     * Constructs a select plan for the table.
     * The plan will use an indexselect, if possible.
     *
     * @return a select plan for the table.
     */
    public Plan makeSelectPlan() {
        Plan p = makeIndexSelect();
        if (p == null)
            p = myplan;
        return addSelectPred(p);
    }

    /**
     * Constructs a join plan of the specified plan
     * and the table.  The plan will use an indexjoin, if possible.
     * (Which means that if an indexselect is also possible,
     * the indexjoin operator takes precedence.)
     * The method returns null if no join is possible.
     *
     * @param current the specified plan
     * @return a join plan of the plan and this table
     */
    public Plan makeJoinPlan(Plan current) { //todo Query Optimiser cost model should be called here to decide which Join0
        Schema currsch = current.schema();
        Predicate joinpred = mypred.joinSubPred(myschema, currsch);
        if (joinpred == null)
            return null;

        //get the Terms of the join operation
        Term joinTerm = joinpred.getMostConstrainingTerm();
        String lhsfield = joinTerm.getLhs().asFieldName();
        String rhsfield = joinTerm.getRhs().asFieldName();
        CondOp condOp = joinTerm.getCondOp();
        if (myschema.hasField(lhsfield)) {
            //need to flip terms
            String temp = lhsfield;
            lhsfield = rhsfield;
            rhsfield = temp;
            condOp = condOp.flip();
        }

        //compile all joins
        Map<Setting.JoinMode, Plan> JoinPlans = new HashMap<>();
        JoinPlans.put(Setting.JoinMode.block, new BlockJoinPlan(tx, current, myplan, lhsfield, condOp, rhsfield));
        JoinPlans.put(Setting.JoinMode.product, new MultibufferProductPlan(tx, current, myplan));
        if (condOp.getVal() == CondOp.types.equals) {
            JoinPlans.put(Setting.JoinMode.hash, new GraceHashJoinPlan(tx, current, myplan, lhsfield, rhsfield));
            JoinPlans.put(Setting.JoinMode.merge, new MergeJoinPlan(tx, current, myplan, lhsfield, condOp, rhsfield, false));
            JoinPlans.put(Setting.JoinMode.index, makeIndexJoin(current, currsch));
        }

        //decide which join to use
        Plan p = null;
        Setting.JoinMode joinMode = Setting.getInstance().getJoinMode();
        if (joinMode.toString().equals("cost")) {
            int cost = Integer.MAX_VALUE;
            for (Plan jp : JoinPlans.values()) {
                if (jp != null && jp.blocksAccessed() < cost &&
                        !(jp instanceof MultibufferProductPlan)) {
                    p = jp;
                    cost = jp.blocksAccessed();
                }
                if (jp != null) {
                    ExecutionPath.getInstance().printScoring(jp.GetEC());
                }
            }
        } else {
            p = JoinPlans.get(joinMode);
        }

        return p != null ? addJoinPred(addSelectPred(p), currsch) : makeProductJoin(current, currsch);
    }

    /**
     * Constructs a product plan of the specified plan and
     * this table.
     *
     * @param current the specified plan
     * @return a product plan of the specified plan and this table
     */
    public Plan makeProductPlan(Plan current) {
        Plan p = addSelectPred(myplan);
        return new MultibufferProductPlan(tx, current, p);
    }

    private Plan makeIndexSelect() {
        for (String fldname : indexes.keySet()) {
            Constant val = mypred.equatesWithConstant(fldname);
            if (val != null) {
                IndexInfo ii = indexes.get(fldname);
//                System.out.println("index on " + fldname + " used");
                return new IndexSelectPlan(myplan, ii, val);
            }
        }
        return null;
    }

    private Plan makeIndexJoin(Plan current, Schema currsch) {
        for (String fldname : indexes.keySet()) {
            String outerfield = mypred.equatesWithField(fldname);
            if (outerfield != null && currsch.hasField(outerfield)) {
                IndexInfo ii = indexes.get(fldname);
                Plan p = new IndexJoinPlan(current, myplan, ii, outerfield);
                return p;
            }
        }
        return null;
    }

    private Plan makeProductJoin(Plan current, Schema currsch) {
        Plan p = makeProductPlan(current);
        return addJoinPred(p, currsch);
    }

    private Plan addSelectPred(Plan p) {
        Predicate selectpred = mypred.selectSubPred(myschema);
        if (selectpred != null && p != null)
            return new SelectPlan(p, selectpred);
        else
            return p;
    }

    private Plan addJoinPred(Plan p, Schema currsch) {//}, String joinfield1, String joinfield2) {
        Predicate joinpred = mypred.joinSubPred(currsch, myschema);
//      joinpred.differenceWith(mypred.relationBetweenField(joinfield1, joinfield2));
        if (joinpred != null && p != null)
            return new SelectPlan(p, joinpred);
        else
            return p;
    }
}
