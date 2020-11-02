package com.scistor.compute.model.remote;

/**
 * @author kaneshiro.J 2020/9/1
 */
public enum StepSourceType {

    /**
     * 数据源
     */
    DataSource("dataSource"),

    /**
     * 系统算子
     */
    System("system"),

    /**
     * 用户自定义
     */
    Custom("custom");


    private final String key;

    StepSourceType(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public static StepSourceType get(String key) {
        for (StepSourceType s : StepSourceType.values()) {
            if (s.key.equals(key)) {
                return s;
            }
        }
        return null;
    }

}
