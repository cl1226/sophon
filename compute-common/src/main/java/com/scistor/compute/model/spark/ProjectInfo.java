package com.scistor.compute.model.spark;

import com.scistor.compute.model.portal.ConnectConfig;
import com.scistor.compute.model.portal.RedisConfig;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProjectInfo implements Serializable {

    private String projectName;
    private String moduleName;

    private List<UDFConfig> registerUDFs = new ArrayList<>();
    private List<ComputeJob> runJobs = new ArrayList<>();

    private SourceAttribute attribute = new SourceAttribute();
    private SinkAttribute sinkAttribute = new SinkAttribute();

    //spark 微批次时间间隔
    private long batchDuration = 10000;
    //样列数据显示数量
    private int showNumbers = 100;
    private boolean isGetSchema = false;
    // Dynamic update applicationConf
    private RedisConfig redisConfig;

    // mysql applicationConf
    private ConnectConfig mysqlConfig;

    //spark and flink default parallelism
    public int parallelism = 2;

    //flink time
    public String timeCharacteristic;

    // cron expression
    public String cron;

    // global parameters
    public Map<String, Object> globalParameterMap = new HashMap<>();

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public String getModuleName() {
        return moduleName;
    }

    public void setModuleName(String moduleName) {
        this.moduleName = moduleName;
    }

    public List<UDFConfig> getRegisterUDFs() {
        return registerUDFs;
    }

    public void setRegisterUDFs(List<UDFConfig> registerUDFs) {
        this.registerUDFs = registerUDFs;
    }

    public List<ComputeJob> getRunJobs() {
        return runJobs;
    }

    public void setRunJobs(List<ComputeJob> runJobs) {
        this.runJobs = runJobs;
    }

    public SourceAttribute getAttribute() {
        return attribute;
    }

    public void setAttribute(SourceAttribute attribute) {
        this.attribute = attribute;
    }

    public SinkAttribute getSinkAttribute() {
        return sinkAttribute;
    }

    public void setSinkAttribute(SinkAttribute sinkAttribute) {
        this.sinkAttribute = sinkAttribute;
    }

    public long getBatchDuration() {
        return batchDuration;
    }

    public void setBatchDuration(long batchDuration) {
        this.batchDuration = batchDuration;
    }

    public int getShowNumbers() {
        return showNumbers;
    }

    public void setShowNumbers(int showNumbers) {
        this.showNumbers = showNumbers;
    }

    public boolean isGetSchema() {
        return isGetSchema;
    }

    public void setGetSchema(boolean getSchema) {
        isGetSchema = getSchema;
    }

    public RedisConfig getRedisConfig() {
        return redisConfig;
    }

    public void setRedisConfig(RedisConfig redisConfig) {
        this.redisConfig = redisConfig;
    }

    public ConnectConfig getMysqlConfig() {
        return mysqlConfig;
    }

    public void setMysqlConfig(ConnectConfig mysqlConfig) {
        this.mysqlConfig = mysqlConfig;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public String getTimeCharacteristic() {
        return timeCharacteristic;
    }

    public void setTimeCharacteristic(String timeCharacteristic) {
        this.timeCharacteristic = timeCharacteristic;
    }

    public String getCron() {
        return cron;
    }

    public void setCron(String cron) {
        this.cron = cron;
    }

    public Map<String, Object> getGlobalParameterMap() {
        return globalParameterMap;
    }

    public void setGlobalParameterMap(Map<String, Object> globalParameterMap) {
        this.globalParameterMap = globalParameterMap;
    }
}
