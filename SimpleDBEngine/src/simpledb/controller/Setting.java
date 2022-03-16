package simpledb.controller;

import simpledb.display.ExecutionPath;
import simpledb.query.CondOp;

import java.sql.Time;
import java.time.Duration;
import java.time.Instant;

public class Setting {
    private static Setting setting;
    public enum JoinMode {
        cost,
        block,
        hash,
        merge,
        product,
        index
    }

    public enum TimeMode {
        timeon,
        timeoff,
    }

    public enum PrintMode {
        printall,
        printnone,
        printresult,
    }

    private JoinMode joinMode;
    private TimeMode timeMode;
    private Instant timeStart;
    private Instant timeEnd;
    private CondOp.types val = null;
    private PrintMode printMode;
    private boolean printResults;

    public static synchronized Setting getInstance(){
        if(setting == null)
            setting = new Setting();
        return setting;
    }

    public Setting() {
        joinMode = JoinMode.cost;
        timeMode = TimeMode.timeoff;
        printResults = true;
    }

    public JoinMode getJoinMode() {
        return joinMode;
    }
    public boolean isTimerOn() {
        return timeMode == TimeMode.timeon;
    }

    private void setJoinMode(String joinModeName) {
        joinMode = JoinMode.valueOf(joinModeName);
    }
    private void setTimeMode(String timeModeName) {
        timeMode = TimeMode.valueOf(timeModeName);
    }
    private void setPrintModeMode(String printModeName) {
        printMode = PrintMode.valueOf(printModeName);
    }

    public void set(String settingName) {
        if (settingName.contains("time")) {
            setTimeMode(settingName);
            return;
        }
        if (settingName.contains("print")) {
            setPrintModeMode(settingName);
            boolean executionPathEnabled = printMode == PrintMode.printall;
            printResults = printMode != PrintMode.printnone;
            ExecutionPath.getInstance().set(executionPathEnabled, executionPathEnabled);
            return;
        }
        setJoinMode(settingName);
    }

    public void setTimeStart() {
        timeStart = Instant.now();
    }

    public void setTimeStop() {
        timeEnd = Instant.now();
    }

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
