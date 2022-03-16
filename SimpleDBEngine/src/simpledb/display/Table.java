package simpledb.display;

import simpledb.plan.Plan;

/**
 * The ExecutionChain class for table plans
 * format: tablename_tbl
 */
public class Table implements ExecutionChain {
    String tablename;
    Plan main;

    /**
     * Creates a table execution chain
     *
     * @param main      the table plan
     * @param tablename the name of the table
     */
    public Table(Plan main, String tablename) {
        this.tablename = tablename;
        this.main = main;
    }

    /**
     * Return the formatted name of the table plan in main.
     *
     * @see ExecutionChain#getName()
     */
    public String getName() {
        return tablename + "_tbl";
    }

    /**
     * Return the formatted string of the execution chain.
     *
     * @see ExecutionChain#toString()
     */
    public String toString() {
        return getName();
    }

    /**
     * Return the total cost up till the table execution chain.
     *
     * @see ExecutionChain#cost()
     */
    public int cost() {
        return main.blocksAccessed();
    }
}
