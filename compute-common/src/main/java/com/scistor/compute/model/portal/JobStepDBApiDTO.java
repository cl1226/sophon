package com.scistor.compute.model.portal;

import java.io.Serializable;
import java.util.Map;

public class JobStepDBApiDTO implements Serializable {
    private static final long serialVersionUID = -8395047133100395202L;
    private String databaseId;
    private String databaseEnglish;
    private DataSourceType databaseType;
    private String avroSchme;
    private String userclassfullname;
    private String csvSplit;
    private DataFormatType decodeType;
    private String tableName;
    private String bootstrapUrl;
    private String topic;
    private String connectionUrl;
    private String groupId;
    private String username;
    private String password;
    private String query = "";
    private String databaseName;
    private String fieldJson;
    private Map<String, String> parameters;
    private String odpsAccessId;
    private String odpsAccessKey;
    private String odpsEndPoint;
    private boolean isKerberos = false;
    private String timeFiled;
    private Long maxOutOfOrderness;

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public String getDatabaseId() {
        return databaseId;
    }

    public void setDatabaseId(String databaseId) {
        this.databaseId = databaseId;
    }

    public String getDatabaseEnglish() {
        return databaseEnglish;
    }

    public void setDatabaseEnglish(String databaseEnglish) {
        this.databaseEnglish = databaseEnglish;
    }

    public DataSourceType getDatabaseType() {
        return databaseType;
    }

    public void setDatabaseType(DataSourceType databaseType) {
        this.databaseType = databaseType;
    }

    public String getAvroSchme() {
        return avroSchme;
    }

    public void setAvroSchme(String avroSchme) {
        this.avroSchme = avroSchme;
    }

    public String getUserclassfullname() {
        return userclassfullname;
    }

    public void setUserclassfullname(String userclassfullname) {
        this.userclassfullname = userclassfullname;
    }

    public String getCsvSplit() {
        return csvSplit;
    }

    public void setCsvSplit(String csvSplit) {
        this.csvSplit = csvSplit;
    }

    public DataFormatType getDecodeType() {
        return decodeType;
    }

    public void setDecodeType(DataFormatType decodeType) {
        this.decodeType = decodeType;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getBootstrapUrl() {
        return bootstrapUrl;
    }

    public void setBootstrapUrl(String bootstrapUrl) {
        this.bootstrapUrl = bootstrapUrl;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getConnectionUrl() {
        return connectionUrl;
    }

    public void setConnectionUrl(String connectionUrl) {
        this.connectionUrl = connectionUrl;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getFieldJson() {
        return fieldJson;
    }

    public void setFieldJson(String fieldJson) {
        this.fieldJson = fieldJson;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    public String getOdpsAccessId() {
        return odpsAccessId;
    }

    public void setOdpsAccessId(String odpsAccessId) {
        this.odpsAccessId = odpsAccessId;
    }

    public String getOdpsAccessKey() {
        return odpsAccessKey;
    }

    public void setOdpsAccessKey(String odpsAccessKey) {
        this.odpsAccessKey = odpsAccessKey;
    }

    public String getOdpsEndPoint() {
        return odpsEndPoint;
    }

    public void setOdpsEndPoint(String odpsEndPoint) {
        this.odpsEndPoint = odpsEndPoint;
    }

    public boolean isKerberos() {
        return isKerberos;
    }

    public void setKerberos(boolean kerberos) {
        isKerberos = kerberos;
    }

    public String getTimeFiled() {
        return timeFiled;
    }

    public void setTimeFiled(String timeFiled) {
        this.timeFiled = timeFiled;
    }

    public Long getMaxOutOfOrderness() {
        return maxOutOfOrderness;
    }

    public void setMaxOutOfOrderness(Long maxOutOfOrderness) {
        this.maxOutOfOrderness = maxOutOfOrderness;
    }
}
