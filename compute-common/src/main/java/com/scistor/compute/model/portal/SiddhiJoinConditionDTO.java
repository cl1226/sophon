package com.scistor.compute.model.portal;

import java.io.Serializable;

public class SiddhiJoinConditionDTO implements Serializable {

    private static final long serialVersionUID = -9038622302485136940L;
    private String fieldNameA;
    private String stepA;
    private String fieldNameB;
    private String stepB;

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public String getFieldNameA() {
        return fieldNameA;
    }

    public void setFieldNameA(String fieldNameA) {
        this.fieldNameA = fieldNameA;
    }

    public String getStepA() {
        return stepA;
    }

    public void setStepA(String stepA) {
        this.stepA = stepA;
    }

    public String getFieldNameB() {
        return fieldNameB;
    }

    public void setFieldNameB(String fieldNameB) {
        this.fieldNameB = fieldNameB;
    }

    public String getStepB() {
        return stepB;
    }

    public void setStepB(String stepB) {
        this.stepB = stepB;
    }
}
