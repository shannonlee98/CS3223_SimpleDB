package simpledb.display;

import simpledb.plan.Plan;

/**
 * The ExecutionChain class for select plans
 * format: childExecutionChain.SELECT([term_list...])
 */
public class Select implements ExecutionChain {
    Plan main;
    ExecutionChain child;
    String preds;

    /**
     * Creates a select execution chain
     *
     * @param main  the selection plan
     * @param child the child execution plan
     * @param preds the predicates of the selection plan
     */
    public Select(Plan main, ExecutionChain child, String preds) {
        this.main = main;
        this.child = child;
        this.preds = preds.replace(" and ", ", ");
    }

    /**
     * Return the formatted name of the select plan in main.
     *
     * @see ExecutionChain#getName()
     */
    public String getName() {
        String rawName = main.getClass().getName();
        int start = rawName.lastIndexOf(".");
        return rawName.substring(start + 1).replace("Plan", "").toUpperCase();
    }

    /**
     * Return the formatted string of the execution chain.
     *
     * @see ExecutionChain#toString()
     */
    public String toString() {
        return child.toString() + "." + getName() + "(" + preds + ")";
    }

    /**
     * Return the total cost up till the select execution chain.
     *
     * @see ExecutionChain#cost()
     */
    public int cost() {
        return main.blocksAccessed();
    }
}
