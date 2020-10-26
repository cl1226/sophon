package com.scistor.compute.model.portal;

public enum  FieldType {

    Integer("int", "整型"),
    Long("long", "长整型"),
    Short("short", "短整型"),
    Float("float", "单精度浮点数"),
    Double("double", "双精度浮点数"),
    String("string", "字符串"),
    Date("date", "日期"),
    Timestamp("timestamp", "时间戳"),
    Boolean("boolean", "布尔"),
    Struct("struct", "结构型"),
    Array("array", "数组"),
    Byte("byte", "字节"),
    Binary("binary", "字节数组");

    private final String key;
    private final String val;

    private FieldType(String key, String val) {
        this.key = key;
        this.val = val;
    }

    public String getKey() {
        return this.key;
    }

    public String getVal() {
        return this.val;
    }

    public static FieldType get(String key) {
        FieldType[] var1 = values();
        int var2 = var1.length;

        for(int var3 = 0; var3 < var2; ++var3) {
            FieldType s = var1[var3];
            if (s.key.equals(key)) {
                return s;
            }
        }

        return null;
    }

}
