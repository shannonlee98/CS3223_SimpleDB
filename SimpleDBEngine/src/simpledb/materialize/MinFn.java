package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

/**
 * The <i>min</i> aggregation function.
 *
 * @author Edward Sciore
 */
public class MinFn implements AggregationFn {
    private String fldname;
    private Constant val;

    /**
     * Create a min aggregation function for the specified field.
     *
     * @param fldname the name of the aggregated field
     */
    public MinFn(String fldname) {
        this.fldname = fldname;
    }

    /**
     * Start a new minimum to be the
     * field value in the current record.
     *
     * @see AggregationFn#processFirst(Scan)
     */
    public void processFirst(Scan s) {
        val = s.getVal(fldname);
    }

    /**
     * Replace the current minimum by the field value
     * in the current record, if it is lower.
     *
     * @see AggregationFn#processNext(Scan)
     */
    public void processNext(Scan s) {
        Constant newval = s.getVal(fldname);
        if (newval.compareTo(val) < 0)
            val = newval;
    }

    /**
     * Return the field's name, prepended by "minof".
     *
     * @see AggregationFn#fieldName()
     */
    public String fieldName() {
        return "minof" + fldname;
    }

    /**
     * Return the field name to be aggregated on.
     *
     * @return the field name to be aggregated on
     */
    public String field() {
        return fldname;
    }

    /**
     * Return the current minimum.
     *
     * @see AggregationFn#value()
     */
    public Constant value() {
        return val;
    }

    /**
     * Return if the aggregated value is always an integer.
     *
     * @return if the aggregated value is always an integer
     */
    public boolean isAlwaysInteger() {
        return false;
    }
}
