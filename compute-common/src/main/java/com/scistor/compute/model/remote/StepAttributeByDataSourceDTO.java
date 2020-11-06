package com.scistor.compute.model.remote;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @author kaneshiro.J 2020/9/29
 */
public class StepAttributeByDataSourceDTO implements Serializable {

    private String id;

    private String chineseName;

    private String englishName;

    private String dataSourceType;

    private Map<String, Object> connectInfo;

    private String source;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getChineseName() {
        return chineseName;
    }

    public void setChineseName(String chineseName) {
        this.chineseName = chineseName;
    }

    public String getEnglishName() {
        return englishName;
    }

    public void setEnglishName(String englishName) {
        this.englishName = englishName;
    }

    public String getDataSourceType() {
        return dataSourceType;
    }

    public void setDataSourceType(String dataSourceType) {
        this.dataSourceType = dataSourceType;
    }

    public Map<String, Object> getConnectInfo() {
        return connectInfo;
    }

    public void setConnectInfo(Map<String, Object> connectInfo) {
        this.connectInfo = connectInfo;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }
}
