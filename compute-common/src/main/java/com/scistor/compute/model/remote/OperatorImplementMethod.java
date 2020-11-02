package com.scistor.compute.model.remote;

/**
 * @author kaneshiro.J 2020/6/28
 */
public enum OperatorImplementMethod {

    /**
     * Java包
     */
    PackageJava("java-package", "package", "Java包"),

    /**
     * spark包
     */
    PackageSpark("spark-package", "package", "Spark包"),

    /**
     * java脚本
     */
    ScriptJava("java-script", "script", "Java脚本"),

    /**
     * scala脚本
     */
    ScriptScala("scala-script", "script", "Scala脚本"),

    /**
     * Sql脚本
     */
    ScriptSql("sql-script", "script", "SQL脚本"),

    /**
     * python命令
     */
    CommandPython("python-command", "command", "Python命令"),

    /**
     * shell命令
     */
    CommandShell("shell-command", "command", "Shell命令");

    private final String key;
    private final String groupKey;
    private final String description;

    OperatorImplementMethod(String key, String groupKey, String description) {
        this.key = key;
        this.groupKey = groupKey;
        this.description = description;
    }

    public String getKey() {
        return key;
    }

    public String getGroupKey() {
        return groupKey;
    }

    public String getDescription() {
        return description;
    }

    public static OperatorImplementMethod get(String key) {
        for (OperatorImplementMethod s : OperatorImplementMethod.values()) {
            if (s.key.equals(key)) {
                return s;
            }
        }
        return null;
    }

}
