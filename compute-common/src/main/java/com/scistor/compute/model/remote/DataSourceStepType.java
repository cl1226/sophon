package com.scistor.compute.model.remote;

/**
 * @author kaneshiro.J 2020/9/1
 */
public enum DataSourceStepType {

    /**
     * 输入数据源
     */
    DataSourceInput("dataSourceInput"),

    /**
     * 输出数据源
     */
    DataSourceOutput("dataSourceOutput");

    private final String key;

    DataSourceStepType(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public static DataSourceStepType get(String key) {
        for (DataSourceStepType s : DataSourceStepType.values()) {
            if (s.key.equals(key)) {
                return s;
            }
        }
        return null;
    }

}
