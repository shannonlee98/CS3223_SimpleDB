package simpledb.query;

import simpledb.parse.BadSyntaxException;

/**
 * The interface corresponding to SQL conditional operators.
 *
 * @author ZhengWen
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
        this.val = fromStr(s);
    }
    public CondOp(types type) {
        this.val = type;
    }

    public CondOp flip() {
        switch (val) {
            case lessThan:
                return new CondOp(types.moreThan);
            case lessThanOrEquals:
                return new CondOp(types.moreThanOrEquals);
            case equals:
                return new CondOp(types.equals);
            case moreThan:
                return new CondOp(types.lessThan);
            case moreThanOrEquals:
                return new CondOp(types.lessThanOrEquals);
            case notEquals:
                return new CondOp(types.notEquals);
            default:
                throw new BadSyntaxException();
        }
    }

    public types getVal() {
        return val;
    }

    /**
     * Throws an exception if the string is not a valid conditional operator.
     *
     * @param s the conditional operator in string format.
     * @return the conditional operator as condOps enum.
     */
    public types fromStr(String s) {
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
                throw new BadSyntaxException();
        }
    }

    public boolean evaluate(Constant c1, Constant c2) {
        switch (val) {
            case lessThan:
                return c1.compareTo(c2) < 0;
            case lessThanOrEquals:
                return c1.compareTo(c2) <= 0;
            case equals:
                return c1.equals(c2);
            case moreThan:
                return c1.compareTo(c2) > 0;
            case moreThanOrEquals:
                return c1.compareTo(c2) >= 0;
            case notEquals:
                return !c1.equals(c2);
            default:
                throw new BadSyntaxException();
        }
    }

    @Override
    public String toString() {
        switch (val) {
            case equals:
                return "=";
            case lessThan:
                return "<";
            case lessThanOrEquals:
                return "<=";
            case moreThan:
                return ">";
            case moreThanOrEquals:
                return ">=";
            case notEquals:
                return "!=";
            default:
                throw new BadSyntaxException();
        }
    }
}
