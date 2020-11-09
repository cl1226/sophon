package com.scistor.compute.model.remote;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @author kaneshiro.J 2020/9/3
 */
public class TransStepDTO implements Serializable {

    /**
     * 子拖入画布生成的唯一标识uuid（画布上的）
     */
    private String id;

    /**
     * 步骤名称（画布上的）
     */
    private String name;

    /**
     * 步骤来源
     */
    private String stepSource;

    /**
     * 数据源步骤类型，与stepSource联动
     * 若stepSource为 DataSource 则dataSourceStepType为 DataSourceStepType 枚举值的key
     * 若为其他值，则dataSourceStepType为空
     */
    private String dataSourceStepType;

    /**
     * 步骤类型，与stepSource联动
     * 1、若stepSource为 DataSource 则stepType为 DataSourceType 枚举值的key
     * 2、若stepSource为 System     则stepType为 SystemOperator 枚举值的key
     * 3、若stepSource为 Custom     则stepType为 OperatorImplementMethod 枚举值的key
     */
    private String stepType;

    /**
     * 步骤id，对应实际的id
     * 例如：
     * 1、若stepSource为 DataSource 则stepId为数据源的id
     * 2、若stepSource为 System     则stepId为 SystemOperator 枚举值的key
     * 3、若stepSource为 Custom     则stepId为 ComputeOperator 的id
     */
    private String stepId;

    /**
     * 算子属性
     */
    private Map<String, Object> stepAttributes;

    private List<StreamFieldDTO> inputFields;

    private List<StreamFieldDTO> outputFields;

    private String stepFrom;

    private TransStrategy strategy;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getStepSource() {
        return stepSource;
    }

    public void setStepSource(String stepSource) {
        this.stepSource = stepSource;
    }

    public String getDataSourceStepType() {
        return dataSourceStepType;
    }

    public void setDataSourceStepType(String dataSourceStepType) {
        this.dataSourceStepType = dataSourceStepType;
    }

    public String getStepType() {
        return stepType;
    }

    public void setStepType(String stepType) {
        this.stepType = stepType;
    }

    public String getStepId() {
        return stepId;
    }

    public void setStepId(String stepId) {
        this.stepId = stepId;
    }

    public Map<String, Object> getStepAttributes() {
        return stepAttributes;
    }

    public void setStepAttributes(Map<String, Object> stepAttributes) {
        this.stepAttributes = stepAttributes;
    }

    public List<StreamFieldDTO> getInputFields() {
        return inputFields;
    }

    public void setInputFields(List<StreamFieldDTO> inputFields) {
        this.inputFields = inputFields;
    }

    public List<StreamFieldDTO> getOutputFields() {
        return outputFields;
    }

    public void setOutputFields(List<StreamFieldDTO> outputFields) {
        this.outputFields = outputFields;
    }

    public String getStepFrom() {
        return stepFrom;
    }

    public void setStepFrom(String stepFrom) {
        this.stepFrom = stepFrom;
    }

    public TransStrategy getStrategy() {
        return strategy;
    }

    public void setStrategy(TransStrategy strategy) {
        this.strategy = strategy;
    }
}
