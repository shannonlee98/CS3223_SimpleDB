package simpledb.display;

import simpledb.plan.Plan;
import simpledb.query.Predicate;

import java.util.List;

//Table.IndexSelect(sname = "alex", grade="B")
public class Select implements ExecutionChain{
    Plan main;
    ExecutionChain child;
    String preds;

    public Select(Plan main, ExecutionChain child, String preds) {
        this.main = main;
        this.child = child;
        this.preds = preds.replace(" and ", ", ");
    }

    @Override
    public String getName() {
        String rawName = main.getClass().getName();
        int start = rawName.lastIndexOf(".");
        return rawName.substring(start+1).replace("Plan", "").toUpperCase();
    }

    @Override
    public String display() {
        return child.display() + "." + getName() + "(" + preds + ")";
    }

    @Override
    public int cost() {
        return main.blocksAccessed();
    }
}
