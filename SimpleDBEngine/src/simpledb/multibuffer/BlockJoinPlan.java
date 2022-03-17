package simpledb.multibuffer;

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

/**
 * The Plan class for the <i>blockjoin</i> operator.
 */
public class BlockJoinPlan implements Plan {
    private Transaction tx;
    private Plan inner, outer;
    private Schema schema = new Schema();
    private CondOp condOp;
    private String joinfieldInner, joinfieldOuter;

    /**
     * Creates a block join plan for the specified queries.
     *
     * @param p1 the plan for the LHS query
     * @param p2 the plan for the RHS query
     * @param tx the calling transaction
     */
    public BlockJoinPlan(Transaction tx, Plan p1, Plan p2, String joinfield1, CondOp condOp, String joinfield2) {
        this.tx = tx;
        int records1 = p1.recordsOutput();
        int records2 = p2.recordsOutput();

        //decide which plan should be the inner and outer based on the number of records
        if (records1 < records2) {
            inner = p2;
            outer = p1;
            this.joinfieldOuter = joinfield1;
            this.joinfieldInner = joinfield2;
            this.condOp = condOp;
        } else {
            inner = p1;
            outer = p2;
            this.joinfieldOuter = joinfield2;
            this.joinfieldInner = joinfield1;
            this.condOp = condOp.flip();
        }

        schema.addAll(p1.schema());
        schema.addAll(p2.schema());
    }

    /**
     * The method first copies the outer plan to a temp table.
     * In the scan, the temp table will be used to create chunks of the outer table
     * in succession to join with the inner plan.
     *
     * @see Plan#open()
     */
    public Scan open() {
        Scan innerscan = inner.open();
        TempTable tt = copyRecordsFrom(outer);
        return new BlockJoinScan(tx, innerscan, tt.tableName(), tt.getLayout(), joinfieldOuter, condOp, joinfieldInner);
    }

    /**
     * Returns an estimate of the number of block accesses
     * required to execute the query. The formula is:
     * <pre> B(blockjoin(p1,p2)) = B(p1) + B(p2)*num of chunk </pre>
     * where the num of chunks = ceiling(B(p1)/(available buffs-2))
     * carry over cost is required to take into account the I/O cost
     * to produce p1 and p2.
     *
     * @see Plan#blocksAccessed()
     */
    public int blocksAccessed() {
        Plan mpInner = new MaterializePlan(tx, inner); // not opened; just for analysis
        Plan mpOuter = new MaterializePlan(tx, outer); // not opened; just for analysis

        int avail = Math.max(1, tx.availableBuffs() - 2);
        int size = mpOuter.blocksAccessed();

        // this guesses at the # of chunks
        int numchunks = (int) Math.ceil(size * 1.0 / avail);

        int carryoverCost = Math.max(inner.blocksAccessed() +
                outer.blocksAccessed() - mpOuter.blocksAccessed() - mpInner.blocksAccessed(), 0);

        return size + numchunks * mpInner.blocksAccessed() + carryoverCost;
    }

    /**
     * Return the number of records in the join.
     * Assuming uniform distribution, the formula is:
     * <pre> R(join(p1,p2)) = R(p1)*R(p2)/max{V(p1,F1),V(p2,F2)}</pre>
     *
     * @see simpledb.plan.Plan#recordsOutput()
     */
    public int recordsOutput() {
        int maxvals = Math.max(inner.distinctValues(joinfieldInner),
                outer.distinctValues(joinfieldOuter));
        return (inner.recordsOutput() * outer.recordsOutput()) / maxvals;
    }

    /**
     * Estimates the distinct number of field values in the product.
     * Since the product does not increase or decrease field values,
     * the estimate is the same as in the appropriate underlying query.
     *
     * @see Plan#distinctValues(String)
     */
    public int distinctValues(String fldname) {
        if (inner.schema().hasField(fldname))
            return inner.distinctValues(fldname);
        else
            return outer.distinctValues(fldname);
    }

    /**
     * Returns the schema of the product,
     * which is the union of the schemas of the underlying queries.
     *
     * @see Plan#schema()
     */
    public Schema schema() {
        return schema;
    }

    private TempTable copyRecordsFrom(Plan p) {
        Scan src = p.open();
        Schema sch = p.schema();
        TempTable t = new TempTable(tx, sch);
        UpdateScan dest = (UpdateScan) t.open();
        while (src.next()) {
            dest.insert();
            for (String fldname : sch.fields())
                dest.setVal(fldname, src.getVal(fldname));
        }
        src.close();
        dest.close();
        return t;
    }

    /**
     * Returns the schema of the index join.
     *
     * @see Plan#getChain()
     */
    public ExecutionChain getChain() {
        return new Join(this, outer.getChain(), inner.getChain(), joinfieldOuter,
                condOp.toString(), joinfieldInner);
    }
}
