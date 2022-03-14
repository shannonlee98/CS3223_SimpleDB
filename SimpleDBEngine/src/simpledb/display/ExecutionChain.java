package simpledb.display;

import simpledb.plan.Plan;

public interface ExecutionChain {
    public String getName();
    public String display();
    public int cost();
}
