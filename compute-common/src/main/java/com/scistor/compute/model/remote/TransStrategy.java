package com.scistor.compute.model.remote;

import java.io.Serializable;

public class TransStrategy implements Serializable {

    private String runMode;

    private String timeUnit;

    public String getRunMode() {
        return runMode;
    }

    public void setRunMode(String runMode) {
        this.runMode = runMode;
    }

    public String getTimeUnit() {
        return timeUnit;
    }

    public void setTimeUnit(String timeUnit) {
        this.timeUnit = timeUnit;
    }
}
