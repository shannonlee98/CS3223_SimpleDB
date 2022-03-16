package simpledb.controller;

import simpledb.display.ExecutionPath;
import simpledb.query.CondOp;

import java.sql.Time;
import java.time.Duration;
import java.time.Instant;

/**
 * The Setting class that controls several global events in the program
 * such as printing of results, printing of query plan and selecting
 * which join to use.
 */
public class Setting {
    //the singleton instance object of the setting class.
    private static Setting setting;

    public enum JoinMode {
        cost,
        block,
        hash,
        merge,
        product,
        index
    }

    public enum PrintMode {
        printall,
        printnone,
        printresult,
    }

    private JoinMode joinMode;
    private Instant timeStart;
    private Instant timeEnd;
    private PrintMode printMode;
    private boolean printResults;

    /**
     * Retrieve the singleton of the setting class.
     * Initialise it if it has not been initialised.
     * @return the initialised singleton instance of the setting class
     */
    public static synchronized Setting getInstance(){
        if(setting == null)
            setting = new Setting();
        return setting;
    }

    /**
     * Implements the setting class.
     * Sets the default settings.
     */
    public Setting() {
        joinMode = JoinMode.cost;
        printResults = true;
    }

    public JoinMode getJoinMode() {
        return joinMode;
    }

    private void setJoinMode(String joinModeName) {
        joinMode = JoinMode.valueOf(joinModeName);
    }

    private void setPrintModeMode(String printModeName) {
        printMode = PrintMode.valueOf(printModeName);
    }

    /**
     * Updates the variables in the setting accordingly to the settingName.
     * @param settingName the setting to be set
     */
    public void set(String settingName) {
        if (settingName.contains("print")) {
            setPrintModeMode(settingName);
            boolean executionPathEnabled = printMode == PrintMode.printall;
            printResults = printMode != PrintMode.printnone;
            ExecutionPath.getInstance().set(executionPathEnabled, executionPathEnabled);
            return;
        }
        setJoinMode(settingName);
    }

    /**
     * Set the start of the stopwatch
     */
    public void setTimeStart() {
        timeStart = Instant.now();
    }

    /**
     * Set the end of the stopwatch
     */
    public void setTimeStop() {
        timeEnd = Instant.now();
    }

    /**
     * Print the time elapsed recorded by the stopwatch
     */
    public void printTimeElapsed() {
        if (timeStart == null || timeEnd == null) {
            System.out.println("timer has not started or ended");
            return;
        }

        Duration timeElapsed = Duration.between(timeStart, timeEnd);
        System.out.println(timeElapsed.getSeconds() + "." + timeElapsed.getNano() + "s");
    }

    public boolean isPrintResults() {
        return printResults;
    }
}
