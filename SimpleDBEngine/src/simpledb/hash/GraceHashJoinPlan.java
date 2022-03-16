package simpledb.hash;

import simpledb.display.ExecutionChain;
import simpledb.display.Join;
import simpledb.materialize.MaterializePlan;
import simpledb.materialize.TempTable;
import simpledb.plan.Plan;
import simpledb.query.CondOp;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

import java.util.ArrayList;
import java.util.List;

/**
 * The Plan class corresponding to the hash join operation
 */
public class GraceHashJoinPlan implements Plan {
    private Plan smaller, larger;
    private Transaction tx;
    private String joinfieldSmaller;
    private String joinfieldLarger;
    private Schema sch = new Schema();
    private int partitions;
    private int estimatedPartitionMultiplier = 1;

    /**
     * Implements the join operator,
     * using the specified LHS and RHS plans.
     *
     * @param tx         the current transaction
     * @param p1         the left-hand plan
     * @param p2         the right-hand plan
     * @param joinfield1 the left-hand field used for joining
     * @param joinfield2 the right-hand field used for joining
     */
    public GraceHashJoinPlan(Transaction tx, Plan p1, Plan p2, String joinfield1, String joinfield2) {
        //we immediately predict the best probing partition based on recordsOutput.
        this.smaller = p1.recordsOutput() > p2.recordsOutput() ? p2 : p1;
        this.larger = p1.recordsOutput() > p2.recordsOutput() ? p1 : p2;

        this.joinfieldSmaller = p1.recordsOutput() > p2.recordsOutput() ? joinfield2 : joinfield1;
        this.joinfieldLarger = p1.recordsOutput() > p2.recordsOutput() ? joinfield1 : joinfield2;

        partitions = Math.max(tx.availableBuffs() - 1, 1);

        //we assume that the hash function splits the records evenly when computing the cost function.
        int hashTableBlockSize = (int) Math.ceil(Math.min(p1.blocksAccessed(),
                p2.blocksAccessed()) * 1.0 / partitions);
        while (hashTableBlockSize > Math.max(tx.availableBuffs() - 2, 1)) { // Divide M by (B-1) until 'M/(B-1) <= B-2'
            estimatedPartitionMultiplier++;
            hashTableBlockSize = (int) Math.ceil(hashTableBlockSize * 1.0 / partitions);
        }

        sch.addAll(p1.schema());
        sch.addAll(p2.schema());
        this.tx = tx;
    }

    /**
     * Opens a grace hash join scan for this query
     *
     * @see Plan#open()
     */
    public Scan open() {
        List<TempTable> smallerPartitions = getPartitions(smaller.open(), smaller.schema(), joinfieldSmaller);
        List<TempTable> largerPartitions = getPartitions(larger.open(), larger.schema(), joinfieldLarger);

        List<Integer> toSplit;

        while (true) {
            toSplit = new ArrayList<>();
            for (int i = 0; i < smallerPartitions.size(); i++) {
                int partitionsize = tx.size(smallerPartitions.get(i).tableName() + ".tbl");
                if (partitionsize > Math.max(tx.availableBuffs() - 2, 1)) {
                    toSplit.add(i);
                }
            }

            if (toSplit.size() == 0) {
                break;
            }

            //add new splitted partitions.
            for (int i : toSplit) {
                smallerPartitions.addAll(
                        getPartitions(
                                smallerPartitions.get(i).open(),
                                smallerPartitions.get(i).getLayout().schema(),
                                joinfieldSmaller));

                largerPartitions.addAll(
                        getPartitions(
                                largerPartitions.get(i).open(),
                                largerPartitions.get(i).getLayout().schema(),
                                joinfieldLarger));
            }

            //remove old splitted partitions.
            for (int i : toSplit) {
                smallerPartitions.remove(i);
                largerPartitions.remove(i);
            }
        }

        return new GraceHashJoinScan(smallerPartitions, joinfieldSmaller, largerPartitions,
                joinfieldLarger);
    }

    /**
     * Estimates the number of block accesses to compute the join.
     * The formula is:
     * <pre> B(hashjoin(p1,p2,idx)) = B(p1) + B(p2) +
     * (B(p1) + B(p2)) * number of partition splitting </pre>
     *
     * @see Plan#blocksAccessed()
     */
    public int blocksAccessed() {
        Plan mpSmaller = new MaterializePlan(tx, smaller); // not opened; just for analysis
        Plan mpLarger = new MaterializePlan(tx, larger); // not opened; just for analysis

        int carryoverCost = Math.max(smaller.blocksAccessed() +
                larger.blocksAccessed() - mpSmaller.blocksAccessed() - mpLarger.blocksAccessed(), 0);

        return mpSmaller.blocksAccessed() + mpLarger.blocksAccessed() +
                2 * (mpSmaller.blocksAccessed() + mpLarger.blocksAccessed()) * estimatedPartitionMultiplier +
                carryoverCost;
    }

    /**
     * Return the number of records in the join.
     * Assuming uniform distribution, the formula is:
     * <pre> R(join(p1,p2)) = R(p1)*R(p2)/max{V(p1,F1),V(p2,F2)}</pre>
     * @see simpledb.plan.Plan#recordsOutput()
     */
    public int recordsOutput() {
        int maxvals = Math.max(smaller.distinctValues(joinfieldSmaller),
                larger.distinctValues(joinfieldLarger));
        return (smaller.recordsOutput() * larger.recordsOutput()) / maxvals;
    }

    /**
     * Estimates the number of distinct values for the
     * specified field.
     *
     * @see Plan#distinctValues(String)
     */
    public int distinctValues(String fldname) {
        if (smaller.schema().hasField(fldname))
            return smaller.distinctValues(fldname);
        else
            return larger.distinctValues(fldname);
    }

    /**
     * Returns the schema of the hash join.
     *
     * @see Plan#schema()
     */
    public Schema schema() {
        return sch;
    }

    /**
     * Split the given scan into partitions in the form of temp tables.
     * @param src the scan to be split
     * @param sch the schema of the scan
     * @param hashonfield the field for the hashcode
     * @return a list of partitions in the form of temp tables.
     */
    private List<TempTable> getPartitions(Scan src, Schema sch, String hashonfield) {

        //open all partitions, write, then close all partitions.
        //open and closing might incur I/O, at least opening probably would.

        List<TempTable> ttList = new ArrayList<>();
        List<UpdateScan> scanList = new ArrayList<>();
        for (int i = 0; i < partitions; i++) {
            ttList.add(new TempTable(tx, sch));
            scanList.add((UpdateScan) ttList.get(i).open());
        }
        while (src.next()) {
            UpdateScan dest = scanList.
                    get(src.getVal(hashonfield).
                            hashCode() % partitions);
            dest.insert();
            for (String fldname : sch.fields()) {
                dest.setVal(fldname, src.getVal(fldname));
            }
        }

        for (int i = 0; i < partitions; i++) {
            scanList.get(i).close();
        }

        src.close();
        return ttList;
    }

    /**
     * Returns the schema of the index join.
     *
     * @see Plan#getChain()
     */
    public ExecutionChain getChain() {
        return new Join(this, smaller.getChain(), larger.getChain(), joinfieldSmaller,
                new CondOp(CondOp.types.equals).toString(), joinfieldLarger);
    }
}