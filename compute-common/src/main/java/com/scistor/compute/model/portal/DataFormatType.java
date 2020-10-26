package com.scistor.compute.model.portal;

public enum DataFormatType {
    Csv("csv", "csv"),
    Avro("avro", "avro"),
    Avro56("avro56", "avro56"),
    Json("json", "json"),
    Parquet("parquet", "parquet"),
    Custom("custom", "自定义");

    private final String key;
    private final String val;

    private DataFormatType(String key, String val) {
        this.key = key;
        this.val = val;
    }

    public String getKey() {
        return this.key;
    }

    public String getVal() {
        return this.val;
    }

    public static DataFormatType get(String key) {
        DataFormatType[] var1 = values();
        int var2 = var1.length;

        for(int var3 = 0; var3 < var2; ++var3) {
            DataFormatType s = var1[var3];
            if (s.key.equals(key)) {
                return s;
            }
        }

        return null;
    }
}
