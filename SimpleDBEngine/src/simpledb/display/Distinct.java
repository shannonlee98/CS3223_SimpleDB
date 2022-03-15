package simpledb.display;

import simpledb.plan.Plan;

import java.util.LinkedHashMap;
import java.util.List;

public class Distinct implements ExecutionChain{
    Plan main;
    ExecutionChain child;
    LinkedHashMap<String, Boolean> distinctfields;

    public Distinct(Plan main, ExecutionChain child, LinkedHashMap<String, Boolean> distinctfields) {
        this.child = child;
        this.distinctfields = distinctfields;
        this.main= main;
    }

    private String displayDistinctFields() {
        String output = "";
        for (String key : distinctfields.keySet()) {
            output += key + ", ";
        }

        return output.substring(0, output.length() - 2);
    }

    @Override
    public String getName() {
        return "DISTINCT";
    }

    @Override
    public String display() {
        return child.display() + "." + getName() +
                "(" + displayDistinctFields() + ")";
    }

    @Override
    public int cost() {
        return main.blocksAccessed();
    }
}
