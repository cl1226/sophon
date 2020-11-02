package com.scistor.compute.model.remote;

/**
 * @author kaneshiro.J 2020/9/29
 */
public class StreamFieldDTO {

    private String fieldName;

    private String streamFieldName;

    private String fieldType;

    private Boolean isConstant;

    private String constantValue;

    private Integer orderNum;

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getStreamFieldName() {
        return streamFieldName;
    }

    public void setStreamFieldName(String streamFieldName) {
        this.streamFieldName = streamFieldName;
    }

    public String getFieldType() {
        return fieldType;
    }

    public void setFieldType(String fieldType) {
        this.fieldType = fieldType;
    }

    public Boolean getConstant() {
        return isConstant;
    }

    public void setConstant(Boolean constant) {
        isConstant = constant;
    }

    public Integer getOrderNum() {
        return orderNum;
    }

    public void setOrderNum(Integer orderNum) {
        this.orderNum = orderNum;
    }

    public String getConstantValue() {
        return constantValue;
    }

    public void setConstantValue(String constantValue) {
        this.constantValue = constantValue;
    }
}
