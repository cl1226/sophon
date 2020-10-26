package com.scistor.compute.model.spark;

public enum OperatorType {
    MAP,
    SQL,
    UDF,
    PIPE,//python shell command
    BATCH,//批量过滤
    PRIVATE,//内置算子
    CEP,
    DATAINPUT,
    DATAOUTPUT,
    JAVA,
    SCALA,
    siddhi_cep,
    SIDDHI;

    @Override
    public String toString() {
        return this.name();
    }
}
