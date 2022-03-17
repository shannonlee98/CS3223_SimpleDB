package simpledb.query;

import simpledb.plan.Plan;
import simpledb.record.*;

import java.util.ArrayList;
import java.util.List;

/**
 * A term is a comparison between two expressions.
 *
 * @author Edward Sciore
 */
public class Term {
    private Expression lhs, rhs;
    private CondOp condOp;

    /**
     * Create a new term that compares two expressions
     * for equality.
     *
     * @param lhs    the LHS expression
     * @param condOp the conditional operator linking the
     *               two expressions
     * @param rhs    the RHS expression
     */
    public Term(Expression lhs, CondOp condOp, Expression rhs) {
        this.lhs = lhs;
        this.condOp = condOp;
        this.rhs = rhs;
    }

    /**
     * Return true if both of the term's expressions
     * evaluate to the same constant,
     * with respect to the specified scan.
     *
     * @param s the scan
     * @return true if both expressions have the same value in the scan
     */
    public boolean isSatisfied(Scan s) {
        Constant lhsval = lhs.evaluate(s);
        Constant rhsval = rhs.evaluate(s);
        return condOp.evaluate(lhsval, rhsval);
    }

    /**
     * Returns a list of any fields in the term
     *
     * @return list of field names in term
     */
    public List<String> getFields() {
        List<String> fields = new ArrayList<>();
        if (lhs.isFieldName()) {
            fields.add(lhs.asFieldName());
        }
        if (rhs.isFieldName()) {
            fields.add(rhs.asFieldName());
        }
        return fields;
    }

    /**
     * Calculate the extent to which selecting on the term reduces
     * the number of records output by a query.
     * For example if the reduction factor is 2, then the
     * term cuts the size of the output in half.
     *
     * @param p the query's plan
     * @return the integer reduction factor.
     */
    public int reductionFactor(Plan p) {
        String lhsName, rhsName;
        if (lhs.isFieldName() && rhs.isFieldName()) {
            lhsName = lhs.asFieldName();
            rhsName = rhs.asFieldName();
            return Math.max(p.distinctValues(lhsName),
                    p.distinctValues(rhsName));
        }
        if (lhs.isFieldName()) {
            lhsName = lhs.asFieldName();
            return p.distinctValues(lhsName);
        }
        if (rhs.isFieldName()) {
            rhsName = rhs.asFieldName();
            return p.distinctValues(rhsName);
        }
        // otherwise, the term equates constants
        if (lhs.asConstant().equals(rhs.asConstant()))
            return 1;
        else
            return Integer.MAX_VALUE;
    }

    /**
     * Determine if this term is of the form "F=c"
     * where F is the specified field and c is some constant.
     * If so, the method returns that constant.
     * If not, the method returns null.
     *
     * @param fldname the name of the field
     * @return either the constant or null
     */
    public Constant equatesWithConstant(String fldname) {
        if (lhs.isFieldName() &&
                lhs.asFieldName().equals(fldname) &&
                !rhs.isFieldName() && condOp.getVal() == CondOp.types.equals)
            return rhs.asConstant();
        else if (rhs.isFieldName() &&
                rhs.asFieldName().equals(fldname) &&
                !lhs.isFieldName() && condOp.getVal() == CondOp.types.equals)
            return lhs.asConstant();
        else
            return null;
    }

    /**
     * Determine if this term is of the form "F1=F2"
     * where F1 is the specified field and F2 is another field.
     * If so, the method returns the name of that field.
     * If not, the method returns null.
     *
     * @param fldname the name of the field
     * @return either the name of the other field, or null
     */
    public String equatesWithField(String fldname) {
        if (lhs.isFieldName() &&
                lhs.asFieldName().equals(fldname) &&
                rhs.isFieldName() && condOp.getVal() == CondOp.types.equals)
            return rhs.asFieldName();
        else if (rhs.isFieldName() &&
                rhs.asFieldName().equals(fldname) &&
                lhs.isFieldName() && condOp.getVal() == CondOp.types.equals)
            return lhs.asFieldName();
        else
            return null;
    }

    /**
     * Return true if both of the term's expressions
     * apply to the specified schema.
     *
     * @param sch the schema
     * @return true if both expressions apply to the schema
     */
    public boolean appliesTo(Schema sch) {
        return lhs.appliesTo(sch) && rhs.appliesTo(sch);
    }

    public CondOp getCondOp() {return condOp;}

    public Expression getLhs() {return lhs;}
    public Expression getRhs() {return rhs;}

    @Override
    public String toString() {
        return lhs.toString() + condOp.toString() + rhs.toString();
    }
}
