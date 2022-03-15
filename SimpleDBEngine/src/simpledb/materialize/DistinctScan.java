package simpledb.materialize;
import simpledb.query.*;

/**
 * The Scan class for the <i>distinct</i> operator.
 * @author Edward Sciore
 */
public class DistinctScan implements Scan {
    private Scan distinct;

    /**
     * Create a sort scan, given a list of 1 or 2 runs.
     * If there is only 1 run, then s2 will be null and
     * hasmore2 will be false.
     * @param distinct the scan of distinct values
     */
    public DistinctScan(Scan distinct) {
        this.distinct = distinct;
    }

    /**
     * Position the scan before the first record in sorted order.
     * Internally, it moves to the first record of each underlying scan.
     * The variable currentscan is set to null, indicating that there is
     * no current scan.
     * @see simpledb.query.Scan#beforeFirst()
     */
    public void beforeFirst() {
        distinct.beforeFirst();
    }

    /**
     * Move to the next record in sorted order.
     * First, the current scan is moved to the next record.
     * Then the lowest record of the two scans is found, and that
     * scan is chosen to be the new current scan.
     * @see simpledb.query.Scan#next()
     */
    public boolean next() {
        return distinct.next();
    }

    /**
     * Close the two underlying scans.
     * @see simpledb.query.Scan#close()
     */
    public void close() {
        distinct.close();
    }

    /**
     * Get the Constant value of the specified field
     * of the current scan.
     * @see simpledb.query.Scan#getVal(java.lang.String)
     */
    public Constant getVal(String fldname) {
        return distinct.getVal(fldname);
    }

    /**
     * Get the integer value of the specified field
     * of the current scan.
     * @see simpledb.query.Scan#getInt(java.lang.String)
     */
    public int getInt(String fldname) {
        return distinct.getInt(fldname);
    }

    /**
     * Get the string value of the specified field
     * of the current scan.
     * @see simpledb.query.Scan#getString(java.lang.String)
     */
    public String getString(String fldname) {
        return distinct.getString(fldname);
    }

    /**
     * Return true if the specified field is in the current scan.
     * @see simpledb.query.Scan#hasField(java.lang.String)
     */
    public boolean hasField(String fldname) {
        return distinct.hasField(fldname);
    }

}
