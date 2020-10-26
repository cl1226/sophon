package com.scistor.compute.model.portal;

import java.io.Serializable;

public class SiddhiFieldDTO implements Serializable {

    private static final long serialVersionUID = -5403077468643240649L;
    private String filedName;
    private String filedType;
    private String stepName;

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public String getFiledName() {
        return filedName;
    }

    public void setFiledName(String filedName) {
        this.filedName = filedName;
    }

    public String getFiledType() {
        return filedType;
    }

    public void setFiledType(String filedType) {
        this.filedType = filedType;
    }

    public String getStepName() {
        return stepName;
    }

    public void setStepName(String stepName) {
        this.stepName = stepName;
    }
}
