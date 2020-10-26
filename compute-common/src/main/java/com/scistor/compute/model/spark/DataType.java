package com.scistor.compute.model.spark;

import org.apache.spark.sql.types.DataTypes;

public enum DataType {
    STRING("string"),
    BOOLEAN("boolean"),
    LONG("long"),
    FLOAT("float"),
    DOUBLE("double"),
    DATE("date"),
    DATETIME("datetime"),
    TIMESTAMP("timestamp"),
    BYTE("byte"),
    SHORT("short"),
    BINARY("binary"),
    INT("int");

    private DataType arrayType;
    private String name;

    DataType(String name) {
        this.name = name;
    }

    public org.apache.spark.sql.types.DataType getSparkDataType(){
        switch (this) {
            case BYTE: return DataTypes.ByteType;
            case STRING: return DataTypes.StringType;
            case BOOLEAN: return DataTypes.BooleanType;
            case LONG: return DataTypes.LongType;
            case FLOAT: return DataTypes.FloatType;
            case DOUBLE: return DataTypes.DoubleType;
            case DATE: return DataTypes.DateType;
            case TIMESTAMP: return DataTypes.TimestampType;
            case SHORT: return DataTypes.ShortType;
            case INT: return DataTypes.IntegerType;
            case DATETIME: return DataTypes.TimestampType;
            case BINARY: return DataTypes.BinaryType;
            default: return null;
        }
    }
}
