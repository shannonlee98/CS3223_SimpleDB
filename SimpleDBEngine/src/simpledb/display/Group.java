package simpledb.display;

import simpledb.materialize.AggregationFn;
import simpledb.plan.Plan;

import java.util.List;

//Table.Group(sname, sid).Select(aggrFields) **
public class Group implements ExecutionChain {
    ExecutionChain child;
    List<String> groupfields;
    List<AggregationFn> aggrfields;
    Plan main;

    public Group(Plan p, ExecutionChain child, List<String> groupfields,
                 List<AggregationFn> aggrfields) {
        this.child = child;
        this.groupfields = groupfields;
        this.aggrfields = aggrfields;
        this.main = p;
    }

    @Override
    public String getName() {
        return "GROUP";
    }

    @Override
    public String display() {
        return child.display() + "." + getName() + "(" +
                groupfields.toString().replace("[", "").
                        replace("]", "") + ")";
    }

    @Override
    public int cost() {
        return main.blocksAccessed();
    }
}
