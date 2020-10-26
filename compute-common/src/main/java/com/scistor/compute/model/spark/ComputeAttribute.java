package com.scistor.compute.model.spark;

import java.util.HashMap;
import java.util.Map;

public class ComputeAttribute extends SourceAttribute {

    private Map<String, ComputeField> outputMapping = new HashMap<>();//table输入 -> table输出 参数映射

    public Map<String, ComputeField> getOutputMapping() {
        return outputMapping;
    }

    public void setOutputMapping(Map<String, ComputeField> outputMapping) {
        this.outputMapping = outputMapping;
    }

    @Override
    public String toString() {
        return "ComputeAttribute{" +
                "sourceType=" + sourceType +
                ", avroSchme='" + avroSchme + '\'' +
                ", fields=" + fields +
                ", time_field='" + time_field + '\'' +
                ", maxOutOfOrderness=" + maxOutOfOrderness +
                ", fieldsSparkJson='" + fieldsSparkJson + '\'' +
                ", userclassfullname='" + userclassfullname + '\'' +
                ", csvSplit='" + csvSplit + '\'' +
                ", tag='" + tag + '\'' +
                ", decodeType=" + decodeType +
                ", sourcenamespace='" + sourcenamespace + '\'' +
                ", table_name='" + table_name + '\'' +
                ", bootstrap_urls='" + bootstrap_urls + '\'' +
                ", zookeeper_url='" + zookeeper_url + '\'' +
                ", topic='" + topic + '\'' +
                ", groupid='" + groupid + '\'' +
                ", connection_url='" + connection_url + '\'' +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", parameters=" + parameters +
                ", query='" + query + '\'' +
                ", odpsAccessId='" + odpsAccessId + '\'' +
                ", odpsAccessKey='" + odpsAccessKey + '\'' +
                ", odpsEndPoint='" + odpsEndPoint + '\'' +
                ", tunnelUrl='" + tunnelUrl + '\'' +
                ", tablePartitions='" + tablePartitions + '\'' +
                ", isKerberos=" + isKerberos +
                ", jaasConfPath='" + jaasConfPath + '\'' +
                '}';
    }
}
