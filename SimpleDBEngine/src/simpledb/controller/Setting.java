package simpledb.controller;

import simpledb.display.ExecutionPath;
import simpledb.query.CondOp;

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
    private JoinMode joinMode;

    private CondOp.types val = null;

    public static synchronized Setting getInstance(){
        if(setting == null)
            setting = new Setting();
        return setting;
    }

    public Setting() {
        joinMode = JoinMode.cost;
    }

    public JoinMode getJoinMode() {
        return joinMode;
    }

    public void setJoinMode(String joinModeName) {
        joinMode = JoinMode.valueOf(joinModeName);
    }
}
