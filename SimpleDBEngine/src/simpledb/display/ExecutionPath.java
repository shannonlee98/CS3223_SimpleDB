package simpledb.display;

import simpledb.plan.Plan;

public class ExecutionPath {
    private static ExecutionPath executionPath;
    private boolean enabled;
    private boolean scoring;

    public static synchronized ExecutionPath getInstance(){
        if(executionPath == null)
            executionPath = new ExecutionPath();
        return executionPath;
    }

    public ExecutionPath() {
        enabled = true;
        scoring = true;
    }

    public void set(boolean enabled, boolean scoring) {
        this.enabled = enabled;
        this.scoring = scoring;
    }

    public void print(ExecutionChain ec) {
        if (enabled) {
            String output = "\tDisplayQueryPlan>> " + ec.display();
            System.out.println("_".repeat(output.length() + 4));
            System.out.println("|" + output + " |");
            System.out.println("¯".repeat(output.length() + 4));
        }
    }

    public void printScoring(ExecutionChain ec) {
        if (scoring) {
            String output = "\tEstimatedPlanCost> " + ec.display() + ": " + ec.cost() + " I/O";
            System.out.println(output);
        }
    }

    public void printScoreSeparator() {
        if (scoring) {
            System.out.println();
        }
    }
}
