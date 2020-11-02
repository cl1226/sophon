package com.scistor.compute.model.remote;

import java.util.List;

/**
 * @author kaneshiro.J 2020/10/30
 */
public class SparkStepDTO {

    private List<String> stepFrom;

    private TransStepDTO stepInfo;

    public List<String> getStepFrom() {
        return stepFrom;
    }

    public void setStepFrom(List<String> stepFrom) {
        this.stepFrom = stepFrom;
    }

    public TransStepDTO getStepInfo() {
        return stepInfo;
    }

    public void setStepInfo(TransStepDTO stepInfo) {
        this.stepInfo = stepInfo;
    }
}
