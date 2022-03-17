package simpledb.hash;

import simpledb.materialize.TempTable;
import simpledb.query.Constant;
import simpledb.query.Scan;

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
public class GraceHashJoinScan implements Scan {
    private String joinfield1;
    private String joinfield2;

    private int partitions;
    private List<TempTable> partitions1;
    private List<TempTable> partitions2;

    private Scan s2;

    private int currentParition;
    boolean allPartitionsClosed;
    private int keyIterator = 0;

    private Map<Constant, ArrayList<Map<String, Constant>>> hashTable;
    private boolean isEmpty;

    /**
     * Creates a block join scan for the specified LHS scan and
     * RHS scan.
     *
     * @param partitions1 the first plan's partitions
     * @param joinfield1  the field from the first table used for joining
     * @param partitions2 the second plan's partitions
     * @param joinfield2  the field from the second table used for joining
     */
    public GraceHashJoinScan(List<TempTable> partitions1,
                             String joinfield1, List<TempTable> partitions2,
                             String joinfield2) {
        this.joinfield1 = joinfield1;
        this.joinfield2 = joinfield2;
        this.partitions = partitions1.size();
        this.partitions1 = partitions1;
        this.partitions2 = partitions2;

        beforeFirst();
    }

    public boolean nextPartition() {
        //close the previous partition scan for s2 if it is open
        if (currentParition >= 0) {
            s2.close();
        }

        currentParition++;

        //if we finished joining all partitions, we are done.
        if (currentParition == partitions) {
            allPartitionsClosed = true;
            return false;
        }
        allPartitionsClosed = false;
        s2 = partitions2.get(currentParition).open();
        s2.beforeFirst();
        if (!s2.next()) {
            isEmpty = !nextPartition();
            return !isEmpty;
        }

        keyIterator = 0;

        Scan s1 = partitions1.get(currentParition).open();
        s1.beforeFirst();

        //initialise hashtable
        hashTable = new HashMap<>();
        while (s1.next()) {
            if (!hashTable.containsKey(s1.getVal(joinfield1))) {
                hashTable.put(s1.getVal(joinfield1), new ArrayList<>());
            }

            Map<String, Constant> row = new HashMap<>();
            for (String field : partitions1.get(0).getLayout().schema().fields()) {
                row.put(field, s1.getVal(field));
            }
            hashTable.get(s1.getVal(joinfield1)).add(row);
        }
        s1.close();

        if (hashTable.isEmpty()) {
            isEmpty = !nextPartition();
            return !isEmpty;
        }

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
        if (isEmpty) {
            return false;
        }
        while (true) {
//            System.out.println("next...");

            //first check if there are duplicate key values in our hashtable
            if (hashTable.keySet().contains(s2.getVal(joinfield2)) &&
                    keyIterator < hashTable.get(s2.getVal(joinfield2)).size()) {
                keyIterator++;
                return true;
            }
            while (s2.next()) {
//                System.out.println("next() s2.next()");
                if (hashTable.keySet().contains(s2.getVal(joinfield2))) {
                    keyIterator = 1;
                    return true;
                }
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
            return hashTable.get(s2.getVal(joinfield2))
                    .get(keyIterator - 1).get(fldname).asInt();
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
            return hashTable.get(s2.getVal(joinfield2)).get(keyIterator - 1).get(fldname);
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
            return hashTable.get(s2.getVal(joinfield2)).
                    get(keyIterator - 1).get(fldname).asString();
        }
    }

    /**
     * Returns true if the field is in the schema.
     *
     * @see Scan#hasField(String)
     */
    public boolean hasField(String fldname) {
        return s2.hasField(fldname) ||
                hashTable.get(s2.getVal(joinfield2)).
                        get(keyIterator - 1).keySet().contains(fldname);
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
