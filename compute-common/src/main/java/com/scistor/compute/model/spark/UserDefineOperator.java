package com.scistor.compute.model.spark;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class UserDefineOperator implements Serializable {
    private String classFullName;
    private String methodName;
    private String jarPath;

    private Map<String, ComputeField> inputMapping = new HashMap<>();//table输入 -> map输入 参数映射
    private Map<String, ComputeField> outputMapping = new HashMap<>();//map输出 -> table输出 参数映射

    public String getClassFullName() {
        return classFullName;
    }

    public void setClassFullName(String classFullName) {
        this.classFullName = classFullName;
    }

    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public String getJarPath() {
        return jarPath;
    }

    public void setJarPath(String jarPath) {
        this.jarPath = jarPath;
    }

    public Map<String, ComputeField> getInputMapping() {
        return inputMapping;
    }

    public void setInputMapping(Map<String, ComputeField> inputMapping) {
        this.inputMapping = inputMapping;
    }

    public Map<String, ComputeField> getOutputMapping() {
        return outputMapping;
    }

    public void setOutputMapping(Map<String, ComputeField> outputMapping) {
        this.outputMapping = outputMapping;
    }
}
