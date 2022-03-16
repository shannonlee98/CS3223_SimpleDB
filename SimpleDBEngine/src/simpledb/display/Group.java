package simpledb.display;

import simpledb.materialize.AggregationFn;
import simpledb.plan.Plan;

import java.util.List;

/**
 * The ExecutionChain class for group plans
 * format: childExecutionChain.GROUP([groupbyfields..]).AGGR([aggregates...])
 */
public class Group implements ExecutionChain {
    ExecutionChain child;
    List<String> groupfields;
    List<AggregationFn> aggrfields;
    Plan main;

    /**
     * Creates a group execution chain
     *
     * @param p           the group plan
     * @param child       the child execution plan
     * @param groupfields the group by fields
     * @param aggrfields  the aggregate fields
     */
    public Group(Plan p, ExecutionChain child, List<String> groupfields,
                 List<AggregationFn> aggrfields) {
        this.child = child;
        this.groupfields = groupfields;
        this.aggrfields = aggrfields;
        this.main = p;
    }

    /**
     * Return the formatted name of the group plan in main.
     *
     * @see ExecutionChain#getName()
     */
    public String getName() {
        return "GROUP";
    }

    /**
     * Return the formatted string of the execution chain.
     *
     * @see ExecutionChain#toString()
     */
    public String toString() {
        String aggr = "";
        for (AggregationFn fn : aggrfields) {
            aggr += (fn.fieldName()) + (", ");
        }
        if (!aggr.equals("")) {
            aggr = aggr.substring(0, aggr.length() - 2);
        }
        String display = child.toString() + "." + getName() + "(" +
                groupfields.toString().replace("[", "").
                        replace("]", "") + ")";
        display += ".AGGR(" + aggr + ")";
        return display;
    }

    /**
     * Return the total cost up till the group execution chain.
     *
     * @see ExecutionChain#cost()
     */
    public int cost() {
        return main.blocksAccessed();
    }
}
