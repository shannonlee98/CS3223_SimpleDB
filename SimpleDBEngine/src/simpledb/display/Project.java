package simpledb.display;

import simpledb.plan.Plan;

import java.util.List;

//Table.Project(sname, grade)
public class Project implements ExecutionChain{
    List<String> fieldsList;
    ExecutionChain child;
    Plan main;

    public Project(Plan main, ExecutionChain child, List<String> fieldsList) {
        this.fieldsList = fieldsList;
        this.main = main;
        this.child = child;
    }

    @Override
    public String getName() {
        return "PROJECT";
    }

    @Override
    public String display() {

        String formattedFieldsList = fieldsList.toString().replace("[", "")
                .replace("]", "");
        return child.display() + "." + getName() + "(" + formattedFieldsList + ")";
    }

    @Override
    public int cost() {
        return main.blocksAccessed();
    }
}
