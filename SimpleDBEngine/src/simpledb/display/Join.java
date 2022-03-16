package simpledb.display;

import simpledb.plan.Plan;

/**
 * The ExecutionChain class for join plans
 * format: [leftExecutionChain JOIN([joinofield1 sign joinfield2]) rightExecutionChain]
 */
public class Join implements ExecutionChain {
    ExecutionChain left;
    ExecutionChain right;
    String leftJoinField;
    String rightJoinField;
    String sign;
    Plan main;

    /**
     * Create a join execution chain
     *
     * @param main           the join plan
     * @param left           the left child execution chain
     * @param right          the right child execution chain
     * @param leftJoinField  the left join field
     * @param sign           the conditional operator of the join
     * @param rightJoinField the right join field
     */
    public Join(Plan main, ExecutionChain left,
                ExecutionChain right, String leftJoinField, String sign, String rightJoinField) {
        this.left = left;
        this.right = right;
        this.leftJoinField = leftJoinField;
        this.rightJoinField = rightJoinField;
        this.main = main;
        this.sign = sign;
    }

    /**
     * Create a product execution chain
     *
     * @param main  the product plan
     * @param left  the left child execution chain
     * @param right the right child execution chain
     */
    public Join(Plan main, ExecutionChain left,
                ExecutionChain right) {
        this.left = left;
        this.right = right;
        this.leftJoinField = null;
        this.rightJoinField = null;
        this.main = main;
        this.sign = null;
    }

    /**
     * Return the formatted name of the join plan in main.
     *
     * @see ExecutionChain#getName()
     */
    public String getName() {
        String rawName = main.getClass().getName();
        int start = rawName.lastIndexOf(".");
        return rawName.substring(start + 1).replace("Plan", "").replace("Join", "JOIN");
    }

    /**
     * Return the formatted string of the execution chain.
     *
     * @see ExecutionChain#toString()
     */
    public String toString() {
        String joininfo = leftJoinField == null || leftJoinField == "" ? "" : "(" + leftJoinField +
                sign + rightJoinField + ")";
        return "[" + left.toString() + " " + this.getName() + joininfo + " " + right.toString() + "]";
    }

    /**
     * Return the total cost up till the join execution chain.
     *
     * @see ExecutionChain#cost()
     */
    public int cost() {
        return main.blocksAccessed();
    }
}
