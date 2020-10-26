package com.scistor.compute.model.portal;

public class JobStepAttributeApiDTO {
    private static final long serialVersionUID = 3824832328789300175L;
    private String fieldName;
    private String fieldNamejson;
    private String streamFieldName;
    private String streamFieldNameJson;
    private String constantValue;
    private FieldType attribute;
    private String objectType;
    private boolean constant = false;
    private Integer xh;
    private String fromPluginsId;
    private String filedChildren;
    private String streamChildren;
    private String label;

    public JobStepAttributeApiDTO() {
    }

    public JobStepAttributeApiDTO(JobStepAttribute attributr) {
        this.fieldName = attributr.getFieldName();
        this.streamFieldName = attributr.getStreamFieldName();
        this.attribute = FieldType.get(attributr.getAttribute());
        this.fromPluginsId = attributr.getFromPluginsId();
        this.objectType = attributr.getObjectType();
        this.constant = "true".equalsIgnoreCase(attributr.getIsConstant());
        this.xh = attributr.getXh();
        this.filedChildren = attributr.getFiledChildren();
        this.streamChildren = attributr.getStreamChildren();
        this.label = attributr.getLabel();
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getFieldNamejson() {
        return fieldNamejson;
    }

    public void setFieldNamejson(String fieldNamejson) {
        this.fieldNamejson = fieldNamejson;
    }

    public String getStreamFieldName() {
        return streamFieldName;
    }

    public void setStreamFieldName(String streamFieldName) {
        this.streamFieldName = streamFieldName;
    }

    public String getStreamFieldNameJson() {
        return streamFieldNameJson;
    }

    public void setStreamFieldNameJson(String streamFieldNameJson) {
        this.streamFieldNameJson = streamFieldNameJson;
    }

    public String getConstantValue() {
        return constantValue;
    }

    public void setConstantValue(String constantValue) {
        this.constantValue = constantValue;
    }

    public FieldType getAttribute() {
        return attribute;
    }

    public void setAttribute(FieldType attribute) {
        this.attribute = attribute;
    }

    public String getObjectType() {
        return objectType;
    }

    public void setObjectType(String objectType) {
        this.objectType = objectType;
    }

    public boolean isConstant() {
        return constant;
    }

    public void setConstant(boolean constant) {
        this.constant = constant;
    }

    public Integer getXh() {
        return xh;
    }

    public void setXh(Integer xh) {
        this.xh = xh;
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
