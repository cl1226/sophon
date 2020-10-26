package com.scistor.compute.model.spark;

import scala.Array;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InvokeInfo implements Serializable {
    private String className;
    private String methodName;
    private String alias;//处理后process as
    private String code;
    private List<ComputeField> processField;//params

    private List<Array<String>> aliasField = new ArrayList<>();

    public InvokeInfo(String className, String methodName, List<ComputeField> processField) {
        this.className = className;
        this.methodName = methodName;
        this.processField = processField;
    }

    public InvokeInfo(String className, String methodName, String alias, List<ComputeField> processField) {
        this.className = className;
        this.methodName = methodName;
        this.alias = alias;
        this.processField = processField;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public List<ComputeField> getProcessField() {
        return processField;
    }

    public void setProcessField(List<ComputeField> processField) {
        this.processField = processField;
    }

    public List<Array<String>> getAliasField() {
        return aliasField;
    }

    public void setAliasField(List<Array<String>> aliasField) {
        this.aliasField = aliasField;
    }
}
