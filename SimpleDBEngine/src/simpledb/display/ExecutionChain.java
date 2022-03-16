package simpledb.display;

import simpledb.plan.Plan;

/**
 * The interface implemented by each execution chain.
 * An execution chain keeps track of the history of a Query Plan so that the
 * entire query plan can be easily displayed.
 * i.e. How different plans added up to form the current plan.
 * Each Query Plan can call an execution chain.
 */
public interface ExecutionChain {
    /**
     * Get the formatted plan name of the current execution chain.
     *
     * @return the formatted plan name of the execution chain.
     */
    public String getName();

    /**
     * Get the string representation of the entire execution chain
     * up to the current execution chain.
     *
     * @return the string representation of the execution chain
     */
    public String toString();

    /**
     * Get the estimated I/O cost to process the execution chain's plan up to
     * the current point.
     *
     * @return the estimated I/O cost.
     */
    public int cost();
}
