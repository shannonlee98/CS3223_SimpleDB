package simpledb.materialize;

import java.util.*;

import simpledb.display.ExecutionChain;
import simpledb.display.Group;
import simpledb.tx.Transaction;
import simpledb.record.Schema;
import simpledb.plan.Plan;
import simpledb.query.*;

/**
 * The Plan class for the <i>groupby</i> operator.
 *
 * @author Edward Sciore
 */
public class GroupByPlan implements Plan {
    private Plan p;
    private List<String> groupfields;
    private List<AggregationFn> aggfns;
    private Schema sch = new Schema();

    /**
     * Create a groupby plan for the underlying query.
     * The grouping is determined by the specified
     * collection of group fields,
     * and the aggregation is computed by the
     * specified collection of aggregation functions.
     *
     * @param p           a plan for the underlying query
     * @param groupfields the group fields
     * @param aggfns      the aggregation functions
     * @param tx          the calling transaction
     */
    public GroupByPlan(Transaction tx, Plan p, List<String> groupfields, List<AggregationFn> aggfns) {
        LinkedHashMap<String, Boolean> mapGroupfields = new LinkedHashMap<>();
        for (String field : groupfields) {
            mapGroupfields.put(field, true);
        }
        this.p = new SortPlan(tx, p, mapGroupfields);
        this.groupfields = groupfields;
        this.aggfns = aggfns;
        for (String fldname : groupfields)
            sch.add(fldname, p.schema());

        // add the aggregate fields based on their innate
        // types and type of the field they are aggregating
        for (AggregationFn fn : aggfns) {
            if (fn.isAlwaysInteger()) {
                sch.addIntField(fn.fieldName());
            } else {
                sch.addField(fn.fieldName(), p.schema().type(fn.field()), p.schema().length(fn.field()));
            }
        }
    }

    /**
     * This method opens a sort plan for the specified plan.
     * The sort plan ensures that the underlying records
     * will be appropriately grouped.
     *
     * @see simpledb.plan.Plan#open()
     */
    public Scan open() {
        Scan s = p.open();
        return new GroupByScan(s, groupfields, aggfns);
    }

    /**
     * Return the number of blocks required to
     * compute the aggregation,
     * which is one pass through the sorted table.
     * It does <i>not</i> include the one-time cost
     * of materializing and sorting the records.
     *
     * @see simpledb.plan.Plan#blocksAccessed()
     */
    public int blocksAccessed() {
        return p.blocksAccessed();
    }

    /**
     * Return the number of groups.  Assuming equal distribution,
     * this is the product of the distinct values
     * for each grouping field.
     *
     * @see simpledb.plan.Plan#recordsOutput()
     */
    public int recordsOutput() {
        int numgroups = 1;
        for (String fldname : groupfields)
            numgroups *= p.distinctValues(fldname);
        return numgroups;
    }

    /**
     * Return the number of distinct values for the
     * specified field.  If the field is a grouping field,
     * then the number of distinct values is the same
     * as in the underlying query.
     * If the field is an aggregate field, then we
     * assume that all values are distinct.
     *
     * @see simpledb.plan.Plan#distinctValues(java.lang.String)
     */
    public int distinctValues(String fldname) {
        if (p.schema().hasField(fldname))
            return p.distinctValues(fldname);
        else
            return recordsOutput();
    }

    /**
     * Returns the schema of the output table.
     * The schema consists of the group fields,
     * plus one field for each aggregation function.
     *
     * @see simpledb.plan.Plan#schema()
     */
    public Schema schema() {
        return sch;
    }

    /**
     * Returns the schema of the index join.
     *
     * @see Plan#getChain()
     */
    public ExecutionChain getChain() {
        return new Group(this, p.getChain(), groupfields, aggfns);
    }
}
