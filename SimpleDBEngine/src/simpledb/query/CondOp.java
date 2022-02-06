package simpledb.query;

import simpledb.parse.BadSyntaxException;

/**
 * The interface corresponding to SQL conditional operators.
 * @author ZhengWen
 *
 */
public class CondOp {
    public enum types {
        lessThan,
        lessThanOrEquals,
        equals,
        moreThan,
        moreThanOrEquals,
        notEquals
    }

    private types val = null;

    public CondOp(String s) {
        this.val = Evaluate(s);
    }

    public types getVal() {
        return val;
    }

    /**
     * Throws an exception if the string is not a valid conditional operator.
     * @param s the conditional operator in string format.
     * @return the conditional operator as condOps enum.
     */
    public types Evaluate(String s) {
        switch (s) {
            case "=":
                return types.equals;
            case "<":
                return types.lessThan;
            case "<=":
                return types.lessThanOrEquals;
            case ">":
                return types.moreThan;
            case ">=":
                return types.moreThanOrEquals;
            case "<>":
            case "!=":
                return types.notEquals;
            default:
                //TODO might need a new exception case here (can reuse?)
                throw new BadSyntaxException();
        }
    }
}
