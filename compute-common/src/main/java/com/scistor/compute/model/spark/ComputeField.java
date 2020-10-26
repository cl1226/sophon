package com.scistor.compute.model.spark;

import java.io.Serializable;

public class ComputeField implements Serializable {
    private String fieldName;
    private DataType dataType;
    private boolean nullable;

    private String structFieldJson;

    private boolean constant = false;
    private String constantValue;

    public ComputeField(String fieldName, DataType dataType) {
        this.fieldName = fieldName;
        this.dataType = dataType;
    }

    public ComputeField(String fieldName, DataType dataType, boolean nullable) {
        this.fieldName = fieldName;
        this.dataType = dataType;
        this.nullable = nullable;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public DataType getDataType() {
        return dataType;
    }

    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public String getStructFieldJson() {
        return structFieldJson;
    }

    public void setStructFieldJson(String structFieldJson) {
        this.structFieldJson = structFieldJson;
    }

    public boolean isConstant() {
        return constant;
    }

    public void setConstant(boolean constant) {
        this.constant = constant;
    }

    public String getConstantValue() {
        return constantValue;
    }

    public void setConstantValue(String constantValue) {
        this.constantValue = constantValue;
    }
}
