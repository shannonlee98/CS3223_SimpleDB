package simpledb.display;

import simpledb.plan.Plan;

import java.util.LinkedHashMap;

/**
 * The ExecutionChain class for distinct plans
 * format: childExecutionChain.DISTINCT([distinct_fields...])
 */
public class Distinct implements ExecutionChain {
    Plan main;
    ExecutionChain child;
    LinkedHashMap<String, Boolean> distinctfields;

    /**
     * Creates the distinct execution chain
     *
     * @param main           the distinct plan
     * @param child          the child execution chain
     * @param distinctfields the fields for distinct
     */
    public Distinct(Plan main, ExecutionChain child, LinkedHashMap<String, Boolean> distinctfields) {
        this.child = child;
        this.distinctfields = distinctfields;
        this.main = main;
    }

    /**
     * Formats the distinct fields into a string
     *
     * @return the formatted string
     */
    private String displayDistinctFields() {
        String output = "";
        for (String key : distinctfields.keySet()) {
            output += key + ", ";
        }

        return output.substring(0, output.length() - 2);
    }

    /**
     * Return the formatted name of the distinct plan in main.
     *
     * @see ExecutionChain#getName()
     */
    public String getName() {
        return "DISTINCT";
    }

    /**
     * Return the formatted string of the execution chain.
     *
     * @see ExecutionChain#toString()
     */
    public String toString() {
        return child.toString() + "." + getName() +
                "(" + displayDistinctFields() + ")";
    }

    /**
     * Return the total cost up till the distinct execution chain.
     *
     * @see ExecutionChain#cost()
     */
    public int cost() {
        return main.blocksAccessed();
    }
}
