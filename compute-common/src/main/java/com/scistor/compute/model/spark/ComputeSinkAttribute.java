package com.scistor.compute.model.spark;

import java.util.HashMap;
import java.util.Map;

public class ComputeSinkAttribute extends SinkAttribute {

    private Map<String, ComputeField> outputMapping = new HashMap<>();//table输入 -> table输出 参数映射

    public Map<String, ComputeField> getOutputMapping() {
        return outputMapping;
    }

    public void setOutputMapping(Map<String, ComputeField> outputMapping) {
        this.outputMapping = outputMapping;
    }

}
