package simpledb.hash;

import simpledb.plan.Plan;
import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.RID;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

import java.util.*;

/**
 * The scan class corresponding to the indexjoin relational
 * algebra operator.
 * The code is very similar to that of ProductScan,
 * which makes sense because an index join is essentially
 * the product of each LHS record with the matching RHS index records.
 *
 * @author Edward Sciore
 */
public class HashJoinScan implements Scan {
    private String joinfield1;
    private String joinfield2;

    private int partitions;
    private ArrayList<Plan> partitions1;
    private ArrayList<Plan> partitions2;

    private Scan s2;

    private List<String> s1fields;
    private int currentParition;
    boolean allPartitionsClosed;

    private Map<Constant, Map<String, Constant>> hashTable;
    //there is a successful match but how can we find the
    //scan in s1.

    /**
     * Need to somehow get the hash function for partitioning the given field.
     * To replicate copies use UpdateScan n maybe Materialize plan to get and create
     * the B - 1 partitions.
     *
     * Can use these two methods for the joining part as well.
     * For the joining part, iterate an entire scan of one side into a hashmap.
     * then, iterate the other scan and return true if u found a match.
     * If the current scan returned false, bring in the next scan.
     */


    /**
     * Creates a block join scan for the specified LHS scan and
     * RHS scan.
     *
     * @param partitions1 the first plan's partitions
     * @param joinfield1  the field from the first table used for joining
     * @param partitions2 the second plan's partitions
     * @param joinfield2  the field from the second table used for joining
     * @param partitions  number of partitions for the hashJoin
     */
    public HashJoinScan(ArrayList<Plan> partitions1,
                        String joinfield1, ArrayList<Plan> partitions2,
                        String joinfield2, int partitions, List<String> s1fields) {
        this.joinfield1 = joinfield1;
        this.joinfield2 = joinfield2;
        this.partitions = partitions;
        this.partitions1 = partitions1;
        this.partitions2 = partitions2;
        this.s1fields = s1fields;

        beforeFirst();
    }

    public boolean nextPartition() {
        if (currentParition >= 0) {
            s2.close();
        }

        currentParition++;

        if (currentParition == partitions) {
            allPartitionsClosed = true;
            return false;
        }
        allPartitionsClosed = false;

        Scan s1 = partitions1.get(currentParition).open();
        s1.beforeFirst();
        s2 = partitions2.get(currentParition).open();
        s2.beforeFirst();

        //initialise hashtable
        hashTable = new HashMap<>();
        while (s1.next()) {
            hashTable.put(s1.getVal(joinfield1), new HashMap<>());
            for (String field : s1fields) {
                hashTable.get(s1.getVal(joinfield1)).put(field, s1.getVal(field));
            }
        }
        s1.close();

        return true;
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
        currentParition = -1;
        nextPartition();
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
        while (true) {
            while (s2.next()) {
                if (hashTable.keySet().contains(s2.getVal(joinfield2))) return true;
            }

            if (!nextPartition()) return false;
        }
    }

    /**
     * Returns the integer value of the specified field.
     *
     * @see Scan#getVal(String)
     */
    public int getInt(String fldname) {
        if (s2.hasField(fldname))
            return s2.getInt(fldname);
        else {
            return hashTable.get(s2.getVal(joinfield2)).get(fldname).asInt();
        }
    }

    /**
     * Returns the Constant value of the specified field.
     *
     * @see Scan#getVal(String)
     */
    public Constant getVal(String fldname) {
        if (s2.hasField(fldname))
            return s2.getVal(fldname);
        else {
            return hashTable.get(s2.getVal(joinfield2)).get(fldname);
        }
    }

    /**
     * Returns the string value of the specified field.
     *
     * @see Scan#getVal(String)
     */
    public String getString(String fldname) {
        if (s2.hasField(fldname))
            return s2.getString(fldname);
        else {
            return hashTable.get(s2.getVal(joinfield2)).get(fldname).asString();
        }
    }

    /**
     * Returns true if the field is in the schema.
     *
     * @see Scan#hasField(String)
     */
    public boolean hasField(String fldname) {
        return s2.hasField(fldname) ||
                hashTable.get(s2.getVal(joinfield2)).keySet().contains(fldname);
    }

    /**
     * Closes the scan by closing its LHS scan and its RHS index.
     *
     * @see Scan#close()
     */
    public void close() {
        if (!allPartitionsClosed)
            s2.close();
    }
}