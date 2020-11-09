package com.scistor.compute.model.remote;

import java.io.Serializable;
import java.util.List;

/**
 * @author kaneshiro.J 2020/10/30
 */
public class SparkTransDTO implements Serializable {

    private String transName;

    private TransStrategy strategy;

    private List<SparkStepDTO> stepList;

    private RedisConfig redisConfig;

    private ConnectConfig mysqlConfig;

    public String getTransName() {
        return transName;
    }

    public void setTransName(String transName) {
        this.transName = transName;
    }

    public TransStrategy getStrategy() {
        return strategy;
    }

    public void setStrategy(TransStrategy strategy) {
        this.strategy = strategy;
    }

    public List<SparkStepDTO> getStepList() {
        return stepList;
    }

    public void setStepList(List<SparkStepDTO> stepList) {
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
}
