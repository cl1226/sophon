package com.scistor.compute.model.spark;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SourceAttribute implements Serializable {

    public SourceType sourceType;//源目标类型
    public String avroSchme;// avro 解析schme信息
    public List<SourceSchme> fields;// 数据 结构信息

    public String time_field;// flink event_time field
    public Long maxOutOfOrderness;// flink event_time 最大允许乱序时间


    public String fieldsSparkJson;// spark 数据 结构信息 json

    public String userclassfullname; //用户自定义结构解析
    public String csvSplit;
    public String tag;// mq subExpression

    public DecodeType decodeType = DecodeType.JSON;//源目标 解码类型
    public String sourcenamespace;//kafka.kafka.topic.topic.*.*.*
    public String table_name;//流注册tablename

    public String bootstrap_urls;
    public String zookeeper_url;
    public String topic;
    public String groupid;//mq consumerId

    public String connection_url;
    public String username;
    public String password;
    public Map<String,String> parameters = new HashMap<>();

    public String query;

    //ODPS config
    public String odpsAccessId = "";
    public String odpsAccessKey = "";
    public String odpsEndPoint = "";
    public String tunnelUrl = "";

    public String tablePartitions = "";

    public boolean isKerberos = true;
    public String jaasConfPath;

    public String databaseName;

    public SourceType getSourceType() {
        return sourceType;
    }

    public void setSourceType(SourceType sourceType) {
        this.sourceType = sourceType;
    }

    public String getAvroSchme() {
        return avroSchme;
    }

    public void setAvroSchme(String avroSchme) {
        this.avroSchme = avroSchme;
    }

    public List<SourceSchme> getFields() {
        return fields;
    }

    public void setFields(List<SourceSchme> fields) {
        this.fields = fields;
    }

    public String getTime_field() {
        return time_field;
    }

    public void setTime_field(String time_field) {
        this.time_field = time_field;
    }

    public Long getMaxOutOfOrderness() {
        return maxOutOfOrderness;
    }

    public void setMaxOutOfOrderness(Long maxOutOfOrderness) {
        this.maxOutOfOrderness = maxOutOfOrderness;
    }

    public String getFieldsSparkJson() {
        return fieldsSparkJson;
    }

    public void setFieldsSparkJson(String fieldsSparkJson) {
        this.fieldsSparkJson = fieldsSparkJson;
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

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public DecodeType getDecodeType() {
        return decodeType;
    }

    public void setDecodeType(DecodeType decodeType) {
        this.decodeType = decodeType;
    }

    public String getSourcenamespace() {
        return sourcenamespace;
    }

    public void setSourcenamespace(String sourcenamespace) {
        this.sourcenamespace = sourcenamespace;
    }

    public String getTable_name() {
        return table_name;
    }

    public void setTable_name(String table_name) {
        this.table_name = table_name;
    }

    public String getBootstrap_urls() {
        return bootstrap_urls;
    }

    public void setBootstrap_urls(String bootstrap_urls) {
        this.bootstrap_urls = bootstrap_urls;
    }

    public String getZookeeper_url() {
        return zookeeper_url;
    }

    public void setZookeeper_url(String zookeeper_url) {
        this.zookeeper_url = zookeeper_url;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getGroupid() {
        return groupid;
    }

    public void setGroupid(String groupid) {
        this.groupid = groupid;
    }

    public String getConnection_url() {
        return connection_url;
    }

    public void setConnection_url(String connection_url) {
        this.connection_url = connection_url;
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

    public Map<String, String> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
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

    public String getTunnelUrl() {
        return tunnelUrl;
    }

    public void setTunnelUrl(String tunnelUrl) {
        this.tunnelUrl = tunnelUrl;
    }

    public String getTablePartitions() {
        return tablePartitions;
    }

    public void setTablePartitions(String tablePartitions) {
        this.tablePartitions = tablePartitions;
    }

    public boolean isKerberos() {
        return isKerberos;
    }

    public void setKerberos(boolean kerberos) {
        isKerberos = kerberos;
    }

    public String getJaasConfPath() {
        return jaasConfPath;
    }

    public void setJaasConfPath(String jaasConfPath) {
        this.jaasConfPath = jaasConfPath;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }
}
