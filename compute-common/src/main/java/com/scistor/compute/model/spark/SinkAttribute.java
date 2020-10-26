package com.scistor.compute.model.spark;

import java.io.Serializable;
import java.util.List;

public class SinkAttribute implements Serializable {
    public SinkType sinkType;// 数据下沉类型
    public String avroSchme;// avro 解析schme信息
    public List<SourceSchme> fields;// 数据 结构信息
    public String fieldsSparkJson;// spark 数据 结构信息 json

    public String csvSplit;
    public String tag;

    public String sinknamespace;

    public SinkFormat sinkFormat;// 数据下沉类型
    public String bootstrap_urls;
    public String zookeeper_url;
    public String topic;
    public String groupid;
    public String sink_connection_username;
    public String sink_connection_password;
    public String tableName;

    public String sinkModel; //overwrite append ignore error
    public String sink_connection_url;
    public String sink_process_class_fullname = "edp.wormhole.sinks.dbsink.Data2DbSink";

    //ODPS config
    public String odpsAccessId = "";
    public String odpsAccessKey = "";
    public String odpsEndPoint = "";
    public String tunnelUrl = "";

    public String tablePartitions = "";

    //预留参数 默认
    public String sink_table_keys;//table 主键
    public String sink_output;
    public String sink_connection_config;
    public String sink_specific_config = "{\"mutation_type\":\"i\"}";
    public int sink_retry_times = 3;
    public int sink_retry_seconds = 300;

    public boolean isKerberos = false;
    public String jaasConfPath;

    public SinkType getSinkType() {
        return sinkType;
    }

    public void setSinkType(SinkType sinkType) {
        this.sinkType = sinkType;
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

    public String getFieldsSparkJson() {
        return fieldsSparkJson;
    }

    public void setFieldsSparkJson(String fieldsSparkJson) {
        this.fieldsSparkJson = fieldsSparkJson;
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

    public String getSinknamespace() {
        return sinknamespace;
    }

    public void setSinknamespace(String sinknamespace) {
        this.sinknamespace = sinknamespace;
    }

    public SinkFormat getSinkFormat() {
        return sinkFormat;
    }

    public void setSinkFormat(SinkFormat sinkFormat) {
        this.sinkFormat = sinkFormat;
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

    public String getSink_connection_username() {
        return sink_connection_username;
    }

    public void setSink_connection_username(String sink_connection_username) {
        this.sink_connection_username = sink_connection_username;
    }

    public String getSink_connection_password() {
        return sink_connection_password;
    }

    public void setSink_connection_password(String sink_connection_password) {
        this.sink_connection_password = sink_connection_password;
    }

    public String getSinkModel() {
        return sinkModel;
    }

    public void setSinkModel(String sinkModel) {
        this.sinkModel = sinkModel;
    }

    public String getSink_connection_url() {
        return sink_connection_url;
    }

    public void setSink_connection_url(String sink_connection_url) {
        this.sink_connection_url = sink_connection_url;
    }

    public String getSink_process_class_fullname() {
        return sink_process_class_fullname;
    }

    public void setSink_process_class_fullname(String sink_process_class_fullname) {
        this.sink_process_class_fullname = sink_process_class_fullname;
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

    public String getSink_table_keys() {
        return sink_table_keys;
    }

    public void setSink_table_keys(String sink_table_keys) {
        this.sink_table_keys = sink_table_keys;
    }

    public String getSink_output() {
        return sink_output;
    }

    public void setSink_output(String sink_output) {
        this.sink_output = sink_output;
    }

    public String getSink_connection_config() {
        return sink_connection_config;
    }

    public void setSink_connection_config(String sink_connection_config) {
        this.sink_connection_config = sink_connection_config;
    }

    public String getSink_specific_config() {
        return sink_specific_config;
    }

    public void setSink_specific_config(String sink_specific_config) {
        this.sink_specific_config = sink_specific_config;
    }

    public int getSink_retry_times() {
        return sink_retry_times;
    }

    public void setSink_retry_times(int sink_retry_times) {
        this.sink_retry_times = sink_retry_times;
    }

    public int getSink_retry_seconds() {
        return sink_retry_seconds;
    }

    public void setSink_retry_seconds(int sink_retry_seconds) {
        this.sink_retry_seconds = sink_retry_seconds;
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

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
