package com.scistor.compute.model.portal;

public enum DataSourceType {

    Kafka("kafka", "kafka"),
    MySQL("mysql", "jdbc"),
    Postgres("postgres", "jdbc"),
    Gbase("gbase", "jdbc"),
    Oracle("oracle", "jdbc"),
    SQLServer("sqlserver", "jdbc"),
    SQLite("sqlite", "jdbc"),
    Hive("hive", "jdbc"),
    HiveJDBC("hive-jdbc", "jdbc"),
    Gaussdb("gaussdb", "jdbc"),
    Hdfs("hdfs", "hdfs"),
    Ftp("ftp", "ftp"),
    Http("http", "http"),
    ElasticSearch("es", "es"),
    CustomJdbc("custom-jdbc", "jdbc");
    private final String key;
    private final String group;

    private DataSourceType(String key, String group) {
        this.key = key;
        this.group = group;
    }

    public String getKey() {
        return this.key;
    }

    public String getGroup() {
        return this.group;
    }

    public static DataSourceType get(String key) {
        DataSourceType[] var1 = values();
        int var2 = var1.length;

        for(int var3 = 0; var3 < var2; ++var3) {
            DataSourceType s = var1[var3];
            if (s.key.equals(key)) {
                return s;
            }
        }

        return null;
    }
}
