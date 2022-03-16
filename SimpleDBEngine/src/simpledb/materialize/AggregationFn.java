package simpledb.materialize;

import simpledb.query.*;

/**
 * The interface implemented by aggregation functions.
 * Aggregation functions are used by the <i>groupby</i> operator.
 *
 * @author Edward Sciore
 */
public interface AggregationFn {

    /**
     * Use the current record of the specified scan
     * to be the first record in the group.
     *
     * @param s the scan to aggregate over.
     */
    void processFirst(Scan s);

    /**
     * Use the current record of the specified scan
     * to be the next record in the group.
     *
     * @param s the scan to aggregate over.
     */
    void processNext(Scan s);

    /**
     * Return the name of the new aggregation field.
     *
     * @return the name of the new aggregation field
     */
    String fieldName();

    /**
     * Return the field name to be aggregated on.
     *
     * @return the field name to be aggregated on
     */
    String field();

    /**
     * Return the computed aggregation value.
     *
     * @return the computed aggregation value
     */
    Constant value();

    /**
     * Return if the aggregated value is always an integer.
     *
     * @return if the aggregated value is always an integer
     */
    boolean isAlwaysInteger();
}
