package simpledb.display;

import simpledb.plan.Plan;

import java.util.LinkedHashMap;

/**
 * The ExecutionChain class for sort plans
 * format: childExecutionChain.SORT([sort_fields...])
 */
public class Sort implements ExecutionChain {
    Plan main;
    ExecutionChain child;
    LinkedHashMap<String, Boolean> sortfields;

    /**
     * Creates a sort execution chain
     *
     * @param main       the sort plan
     * @param child      the child execution chain
     * @param sortfields the pairs of fieldname and asc or dsc
     */
    public Sort(Plan main, ExecutionChain child, LinkedHashMap<String, Boolean> sortfields) {
        this.main = main;
        this.sortfields = sortfields;
        this.child = child;
    }

    /**
     * Format the sort fields into a string
     *
     * @return a string representing the list of sorted fields and if they
     * are asc or dsc.
     */
    private String displaySortFields() {
        String output = "";
        for (String key : sortfields.keySet()) {
            output += key + " " + (sortfields.get(key) ? "ASC" : "DSC") + ", ";
        }

        if (output.length() >= 2)
            return output.substring(0, output.length() - 2);
        return output;
    }

    /**
     * Return the formatted name of the sort plan in main.
     *
     * @see ExecutionChain#getName()
     */
    public String getName() {
        return "SORT";
    }

    /**
     * Return the formatted string of the execution chain.
     *
     * @see ExecutionChain#toString()
     */
    public String toString() {
        return child.toString() + "." + getName() + "(" + displaySortFields() + ")";
    }

    /**
     * Return the total cost up till the sort execution chain.
     *
     * @see ExecutionChain#cost()
     */
    public int cost() {
        return main.blocksAccessed();
    }
}
