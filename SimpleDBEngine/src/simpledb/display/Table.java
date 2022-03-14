package simpledb.display;

import simpledb.plan.Plan;

public class Table implements ExecutionChain {
    String tablename;
    Plan main;

    public Table(Plan main, String tablename) {
        this.tablename = tablename;
        this.main = main;
    }

    @Override
    public String getName() {
        return tablename + "_tbl";
    }

    @Override
    public String display() {
        return getName();
    }

    @Override
    public int cost() {
        return main.blocksAccessed();
    }
}
