package com.scistor.compute.model.portal;

import java.io.Serializable;

public class JobStepAttribute implements Serializable {
    private static final long serialVersionUID = -5107758892308628146L;
    private String id;
    private String jobId;
    private String stepId;
    private Integer nr;
    private Integer xh;
    private String fieldName;
    private String streamFieldName;
    private String attributeType;
    private String attribute;
    private String objectType;
    private String isConstant;
    private String fromPluginsId;
    private String filedChildren;
    private String streamChildren;
    private String label;

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getStepId() {
        return stepId;
    }

    public void setStepId(String stepId) {
        this.stepId = stepId;
    }

    public Integer getNr() {
        return nr;
    }

    public void setNr(Integer nr) {
        this.nr = nr;
    }

    public Integer getXh() {
        return xh;
    }

    public void setXh(Integer xh) {
        this.xh = xh;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getStreamFieldName() {
        return streamFieldName;
    }

    public void setStreamFieldName(String streamFieldName) {
        this.streamFieldName = streamFieldName;
    }

    public String getAttributeType() {
        return attributeType;
    }

    public void setAttributeType(String attributeType) {
        this.attributeType = attributeType;
    }

    public String getAttribute() {
        return attribute;
    }

    public void setAttribute(String attribute) {
        this.attribute = attribute;
    }

    public String getObjectType() {
        return objectType;
    }

    public void setObjectType(String objectType) {
        this.objectType = objectType;
    }

    public String getIsConstant() {
        return isConstant;
    }

    public void setIsConstant(String isConstant) {
        this.isConstant = isConstant;
    }

    public String getFromPluginsId() {
        return fromPluginsId;
    }

    public void setFromPluginsId(String fromPluginsId) {
        this.fromPluginsId = fromPluginsId;
    }

    public String getFiledChildren() {
        return filedChildren;
    }

    public void setFiledChildren(String filedChildren) {
        this.filedChildren = filedChildren;
    }

    public String getStreamChildren() {
        return streamChildren;
    }

    public void setStreamChildren(String streamChildren) {
        this.streamChildren = streamChildren;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }
}
