package simpledb.display;

import simpledb.plan.Plan;

import java.util.List;

/**
 * The ExecutionChain class for projection plans
 * format: childExecutionChain.PROJECT([projection_fields...])
 */
public class Project implements ExecutionChain {
    List<String> fieldsList;
    ExecutionChain child;
    Plan main;

    /**
     * Creates a project execution chain
     *
     * @param main       the projection plan
     * @param child      the child execution chain
     * @param fieldsList the list of fields to be projected
     */
    public Project(Plan main, ExecutionChain child, List<String> fieldsList) {
        this.fieldsList = fieldsList;
        this.main = main;
        this.child = child;
    }

    /**
     * Return the formatted name of the project plan in main.
     *
     * @see ExecutionChain#getName()
     */
    public String getName() {
        return "PROJECT";
    }

    /**
     * Return the formatted string of the execution chain.
     *
     * @see ExecutionChain#toString()
     */
    public String toString() {

        String formattedFieldsList = fieldsList.toString().replace("[", "")
                .replace("]", "");
        return child.toString() + "." + getName() + "(" + formattedFieldsList + ")";
    }

    /**
     * Return the total cost up till the project execution chain.
     *
     * @see ExecutionChain#cost()
     */
    public int cost() {
        return main.blocksAccessed();
    }
}
