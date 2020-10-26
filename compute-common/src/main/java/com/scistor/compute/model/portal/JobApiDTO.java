package com.scistor.compute.model.portal;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class JobApiDTO implements Serializable {

    private String name = "";
    private String groupId = "";
    private Boolean debug = false;
    private String siddhi = "";
    private long batchDuration = 10000L;
    private List<JobStepApiDTO> stepList = new ArrayList();
    private RedisConfig redisConfig;
    private ConnectConfig mysqlConfig;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public Boolean getDebug() {
        return debug;
    }

    public void setDebug(Boolean debug) {
        this.debug = debug;
    }

    public String getSiddhi() {
        return siddhi;
    }

    public void setSiddhi(String siddhi) {
        this.siddhi = siddhi;
    }

    public long getBatchDuration() {
        return batchDuration;
    }

    public void setBatchDuration(long batchDuration) {
        this.batchDuration = batchDuration;
    }

    public List<JobStepApiDTO> getStepList() {
        return stepList;
    }

    public void setStepList(List<JobStepApiDTO> stepList) {
        this.stepList = stepList;
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

    @Override
    public String toString() {
        return "JobApiDTO{" +
                "name='" + name + '\'' +
                ", groupId='" + groupId + '\'' +
                ", debug=" + debug +
                ", siddhi='" + siddhi + '\'' +
                ", batchDuration=" + batchDuration +
                ", stepList=" + stepList +
                ", redisConfig=" + redisConfig +
                ", mysqlConfig=" + mysqlConfig +
                '}';
    }
}
