package simpledb.materialize;

import simpledb.display.Distinct;
import simpledb.display.ExecutionChain;
import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class DistinctPlan implements Plan {
    private Transaction tx;
    private Plan p;
    private Schema sch;
    private RecordComparator comp;

    /**
     * Create a Distinct Plan for the underlying query.
     * @param tx the calling transaction
     * @param p a plan for the underlying query
     * @param fields the select fields of the query
     */
    public DistinctPlan(Transaction tx, Plan p, List<String> fields) {
        this.tx = tx;
        this.p = p;
        LinkedHashMap<String, Boolean> fieldmap = new LinkedHashMap<>();
        for (String field : fields) {
            fieldmap.put(field, true);
        }
        this.sch = p.schema();
        this.comp = new RecordComparator(fieldmap);
    }

    /**
     * This method is where most of the action is.
     * Up to 2 sorted temporary tables are created,
     * and are passed into SortScan for final merging.
     * @see simpledb.plan.Plan#open()
     */
    public Scan open() {
        Scan src = p.open();
        Scan srcPlus = p.open();
        List<TempTable> runs = splitIntoRuns(src);
        src.close();
        while (runs.size() > 1)
            runs = doDistinctMergeIteration(runs);
        if (runs.size() > 0) {
            srcPlus = runs.get(0).open();
        }
        return srcPlus;
    }

    /**
     * @param src scan to split
     * @return list of sorted runs
     */
    public List<TempTable> splitIntoRuns(Scan src) {
        List<TempTable> temps = new ArrayList<>();
        src.beforeFirst();
        if (!src.next())
            return temps;
        TempTable currenttemp = new TempTable(tx, sch);
        temps.add(currenttemp);
        UpdateScan currentscan = currenttemp.open();
        while (copy(src, currentscan)) {
            if (comp.compare(src, currentscan) < 0) {
                currentscan.close();
                currenttemp = new TempTable(tx, sch);
                temps.add(currenttemp);
                currentscan = currenttemp.open();
            }
        }
        currentscan.close();
        return temps;
    }


    /**
     * @param runs list of sorted runs
     * @return merged runs
     */
    private List<TempTable> doDistinctMergeIteration(List<TempTable> runs) {
        List<TempTable> result = new ArrayList<>();
        while (runs.size() > 1) {
            TempTable p1 = runs.remove(0);
            TempTable p2 = runs.remove(0);
            result.add(mergeTwoDistinctRuns(p1, p2));
        }
        if (runs.size() == 1)
            result.add(runs.get(0));
        return result;
    }

    /**
     * @param p1 first run
     * @param p2 second run
     * @return merged run
     */
    private TempTable mergeTwoDistinctRuns(TempTable p1, TempTable p2) {
        Scan src1 = p1.open();
        Scan src2 = p2.open();
        TempTable result = new TempTable(tx, sch);
        UpdateScan dest = result.open();

        boolean hasmore1 = src1.next();
        boolean hasmore2 = src2.next();

        // First update of dest scan
        if (hasmore1 && hasmore2) {
            if (comp.compare(src1, src2) < 0)
                hasmore1 = copy(src1, dest);
            else
                hasmore2 = copy(src2, dest);
        }

        // Compare until one run has no more values
        while (hasmore1 && hasmore2) {
            if (comp.compare(src1, src2) < 0) {
                if (isDifferent(src1, dest))
                    hasmore1 = copy(src1, dest);
                else
                    hasmore1 = src1.next();
            } else if (isDifferent(src2, dest))
                hasmore2 = copy(src2, dest);
            else
                hasmore2 = src2.next();
        }

        // Add remaining values from remaining run
        if (hasmore1)
            while (hasmore1)
                if (isDifferent(src1, dest))
                    hasmore1 = copy(src1, dest);
                else
                    hasmore1 = src1.next();
        else
            while (hasmore2)
                if (isDifferent(src2, dest))
                    hasmore2 = copy(src2, dest);
                else
                    hasmore2 = src2.next();

        src1.close();
        src2.close();
        dest.close();
        return result;
    }

    /**
     * Returns an estimate of the number of block accesses
     * that will occur when the scan is read to completion.
     *
     * @return the estimated number of block accesses
     */
    public int blocksAccessed() {
        return p.blocksAccessed(); // TODO: plus cost of merging runs
    }

    /**
     * Returns an estimate of the number of records
     * in the query's output table.
     *
     * @return the estimated number of output records
     */
    public int recordsOutput() {
        return p.recordsOutput(); // TODO: need to calculate
    }

    /**
     * Returns an estimate of the number of distinct values
     * for the specified field in the query's output table.
     *
     * @param fldname the name of a field
     * @return the estimated number of distinct field values in the output
     */
    public int distinctValues(String fldname) {
        return p.distinctValues(fldname);
    }

    /**
     * Returns the schema of the query.
     *
     * @return the query's schema
     */
    public Schema schema() {
        return sch;
    }

        /**
     * Returns the schema of the index join.
     *
     * @see Plan#getChain()
     */
    public ExecutionChain getChain() {
        return new Distinct(this, p.getChain(), comp.fields);
    }

    /**
     * Copy source scan to destination scan
     * @param src scan to copy
     * @param dest scan to paste
     * @return true if src has next
     */
    private boolean copy(Scan src, UpdateScan dest) {
        dest.insert();
        for (String fldname : sch.fields()) {
            dest.setVal(fldname, src.getVal(fldname));
        }
        return src.next();
    }

    /**
     * @param src1 first scan
     * @param src2 second scan
     * @return True if scans are different
     */
    private boolean isDifferent(Scan src1, Scan src2) {
        if (src1 == null || src2 == null)
            return false;
        return comp.compare(src1, src2) != 0;
    }
}
