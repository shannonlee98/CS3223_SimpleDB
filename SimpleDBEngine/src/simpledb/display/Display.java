package simpledb.display;

/**
 * This class manages the output for displaying the query plan
 * through execution chains.
 * It also formats the total cost of the plans attached to an execution chain
 */
public class Display {
    private static Display executionPath;
    private boolean enabled;
    private boolean scoring;

    /**
     * Get the singleton instance of the display class.
     * If it has not been initialised, initialise it.
     *
     * @return the singleton instance.
     */
    public static synchronized Display getInstance() {
        if (executionPath == null)
            executionPath = new Display();
        return executionPath;
    }

    /**
     * Create the display class and set the default setting values
     */
    public Display() {
        enabled = true;
        scoring = true;
    }

    /**
     * Set the display settings
     *
     * @param enabled set if display query plan is enabled
     * @param scoring set if query plan cost is enabled
     */
    public void set(boolean enabled, boolean scoring) {
        this.enabled = enabled;
        this.scoring = scoring;
    }

    /**
     * Print the execution chain to the terminal
     *
     * @param ec the execution chain to be printed
     */
    public void print(ExecutionChain ec) {
        if (enabled) {
            String output = "\tDisplayQueryPlan>> " + ec.toString();
            System.out.println("_".repeat(output.length() + 4));
            System.out.println("|" + output + " |");
            System.out.println("¯".repeat(output.length() + 4));
        }
    }

    /**
     * Print the execution chain and it's total cost
     *
     * @param ec the execution chain
     */
    public void printScoring(ExecutionChain ec) {
        if (scoring) {
            String output = "\tEstimatedPlanCost> " + ec.toString() + ": " + ec.cost() + " I/O";
            System.out.println(output);
        }
    }

    /**
     * Prints a line separator for terminal outputs of this class
     */
    public void printScoreSeparator() {
        if (scoring) {
            System.out.println();
        }
    }
}
