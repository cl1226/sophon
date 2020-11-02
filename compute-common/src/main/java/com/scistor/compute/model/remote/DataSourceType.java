package com.scistor.compute.model.remote;

/**
 * @author kaneshiro.J 2019/3/29
 */
public enum DataSourceType {

    /**
     * Kafka
     */
    Http("http", "http"),

    /**
     * Kafka
     */
    Kafka("kafka", "kafka"),

    /**
     * MySQL
     */
    MySQL("mysql", "jdbc"),

    /**
     * Postgres
     */
    Postgres("postgres", "jdbc"),

    /**
     * Gbase
     */
    Gbase("gbase", "jdbc"),

	/**
	 * Oracle
	 */
	Oracle("oracle", "jdbc"),

    /**
     * SQLServer
     */
    SQLServer("sqlserver", "jdbc"),

    /**
     * SQLite
     */
    SQLite("sqlite", "jdbc"),

	/**
	 * Hive
	 */
	Hive("hive", "jdbc"),

    /**
     * HiveJDBC
     */
    HiveJDBC("hive-jdbc", "jdbc"),

    /**
     * GaussDB
     */
    Gaussdb("gaussdb", "jdbc"),

	/**
	 * Hdfs
	 */
	Hdfs("hdfs", "hdfs"),

    /**
     * Ftp
     */
    Ftp("ftp", "ftp"),

    /**
     * Hdfs
     */
    ElasticSearch("es", "es"),

    /**
     * 自定义Jdbc
     */
    CustomJdbc("custom-jdbc", "jdbc");

    private final String key;
    private final String group;

    DataSourceType(String key, String group) {
        this.key = key;
        this.group = group;
    }

    public String getKey() {
        return key;
    }

    public String getGroup() {
        return group;
    }

    public static DataSourceType get(String key) {
        for (DataSourceType s : DataSourceType.values()) {
            if (s.key.equals(key)) {
                return s;
            }
        }
        return null;
    }
}
