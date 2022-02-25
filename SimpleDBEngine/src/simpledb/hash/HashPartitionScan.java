package simpledb.hash;

import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The scan class corresponding to the indexjoin relational
 * algebra operator.
 * The code is very similar to that of ProductScan,
 * which makes sense because an index join is essentially
 * the product of each LHS record with the matching RHS index records.
 *
 * @author Edward Sciore
 */
public class HashPartitionScan implements Scan {
    int hash;
    int partitions;
    String hashonfield;
    Scan s;

    /**
     * Creates a hash partition scan
     *
     * @param s the src scan
     * @param partitions      number of partitions
     * @param hashonfield the field to be hashed.
     */
    public HashPartitionScan(Scan s, int hash, int partitions, String hashonfield) {
        this.s = s;
        this.hash = hash;
        this.partitions = partitions;
        this.hashonfield = hashonfield;

        beforeFirst();
    }

    /**
     * Positions the scan before the first record.
     * That is, the LHS scan will be positioned at its
     * first record, and the RHS will be positioned
     * before the first record for the join value.
     *
     * @see Scan#beforeFirst()
     */
    public void beforeFirst() {
        s.beforeFirst();
    }

    /**
     * Moves the scan to the next record.
     * The method moves to the next RHS record, if possible.
     * Otherwise, it moves to the next LHS record and the
     * first RHS record.
     * If there are no more LHS records, the method returns false.
     *
     * @see Scan#next()
     */

    public boolean next() {
        while (s.next()) {
            if (s.getVal(hashonfield).hashCode() % partitions == hash) {
                return true;
            };
        }

        return false;
    }

    /**
     * Returns the integer value of the specified field.
     *
     * @see Scan#getVal(String)
     */
    public int getInt(String fldname) {
        return s.getInt(fldname);
    }

    /**
     * Returns the Constant value of the specified field.
     *
     * @see Scan#getVal(String)
     */
    public Constant getVal(String fldname) {
        return s.getVal(fldname);
    }

    /**
     * Returns the string value of the specified field.
     *
     * @see Scan#getVal(String)
     */
    public String getString(String fldname) {
        return s.getString(fldname);
    }

    /**
     * Returns true if the field is in the schema.
     *
     * @see Scan#hasField(String)
     */
    public boolean hasField(String fldname) {
        return s.hasField(fldname);
    }

    /**
     * Closes the scan by closing its LHS scan and its RHS index.
     *
     * @see Scan#close()
     */
    public void close() {
        s.close();
    }
}
