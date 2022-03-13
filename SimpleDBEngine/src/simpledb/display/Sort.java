package simpledb.display;

import simpledb.plan.Plan;

import java.util.LinkedHashMap;

//Table.Sort(sortfields)
public class Sort implements ExecutionChain{
    Plan main;
    ExecutionChain child;
    LinkedHashMap<String, Boolean> sortfields;

    public Sort(Plan main, ExecutionChain child, LinkedHashMap<String, Boolean> sortfields) {
        this.main = main;
        this.sortfields = sortfields;
        this.child = child;
    }

    private String displaySortFields() {
        String output = "";
        for (String key : sortfields.keySet()) {
            output += key + " " + (sortfields.get(key) ? "ASC" : "DSC") + ", ";
        }

        return output.substring(0, output.length() - 2);
    }

    @Override
    public String getName() {
        return "SORT";
    }

    @Override
    public String display() {
        return child.display() + "." + getName() + "("+ displaySortFields() +")";
    }

    @Override
    public int cost() {
        return main.blocksAccessed();
    }
}
