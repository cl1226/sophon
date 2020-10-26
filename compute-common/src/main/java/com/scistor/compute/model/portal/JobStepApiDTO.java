package com.scistor.compute.model.portal;

import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JobStepApiDTO implements Serializable {
    private static final long serialVersionUID = 3344994292251840854L;
    private String stepId = "";
    private String name = "";
    private String pluginsId = "";
    private String pluginsName = "";
    private String pluginsClass = "";
    private String pluginsGroup = "";
    private String implementMethod = "";
    private String pluginsType = "";
    private String pluginsFunctionName = "";
    private String pluginsUrl = "";
    private String flag = "";
    private List<String> fromSteps = new ArrayList();
    private Map<String, List<JobStepAttributeApiDTO>> input = new HashMap();
    private Map<String, List<JobStepAttributeApiDTO>> output = new HashMap();
    private String databaseEnglish = "";
    private JobStepDBApiDTO database;
    private List<String> queryList = Lists.newArrayList();
    private String code = "";
    private String cep = "";
    private String siddhicep;
    private String siddhiSqlType;
    private String siddhiSql;
    private String shellFile = "";
    private List<String> thinks = new ArrayList();
    private List<String> aggCols = new ArrayList();
    private boolean agg = false;
    private List<String> jarList = new ArrayList();
    private Map<String, String> attrs = new HashMap();
    private String label;
    private List<String> siddhiJoinStepsId;
    private List<SiddhiFieldDTO> siddhiJoinFields;
    private List<SiddhiJoinConditionDTO> siddhiJoinCondition;

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public String getStepId() {
        return stepId;
    }

    public void setStepId(String stepId) {
        this.stepId = stepId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPluginsId() {
        return pluginsId;
    }

    public void setPluginsId(String pluginsId) {
        this.pluginsId = pluginsId;
    }

    public String getPluginsName() {
        return pluginsName;
    }

    public void setPluginsName(String pluginsName) {
        this.pluginsName = pluginsName;
    }

    public String getPluginsClass() {
        return pluginsClass;
    }

    public void setPluginsClass(String pluginsClass) {
        this.pluginsClass = pluginsClass;
    }

    public String getPluginsGroup() {
        return pluginsGroup;
    }

    public void setPluginsGroup(String pluginsGroup) {
        this.pluginsGroup = pluginsGroup;
    }

    public String getImplementMethod() {
        return implementMethod;
    }

    public void setImplementMethod(String implementMethod) {
        this.implementMethod = implementMethod;
    }

    public String getPluginsType() {
        return pluginsType;
    }

    public void setPluginsType(String pluginsType) {
        this.pluginsType = pluginsType;
    }

    public String getPluginsFunctionName() {
        return pluginsFunctionName;
    }

    public void setPluginsFunctionName(String pluginsFunctionName) {
        this.pluginsFunctionName = pluginsFunctionName;
    }

    public String getPluginsUrl() {
        return pluginsUrl;
    }

    public void setPluginsUrl(String pluginsUrl) {
        this.pluginsUrl = pluginsUrl;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public List<String> getFromSteps() {
        return fromSteps;
    }

    public void setFromSteps(List<String> fromSteps) {
        this.fromSteps = fromSteps;
    }

    public Map<String, List<JobStepAttributeApiDTO>> getInput() {
        return input;
    }

    public void setInput(Map<String, List<JobStepAttributeApiDTO>> input) {
        this.input = input;
    }

    public Map<String, List<JobStepAttributeApiDTO>> getOutput() {
        return output;
    }

    public void setOutput(Map<String, List<JobStepAttributeApiDTO>> output) {
        this.output = output;
    }

    public String getDatabaseEnglish() {
        return databaseEnglish;
    }

    public void setDatabaseEnglish(String databaseEnglish) {
        this.databaseEnglish = databaseEnglish;
    }

    public JobStepDBApiDTO getDatabase() {
        return database;
    }

    public void setDatabase(JobStepDBApiDTO database) {
        this.database = database;
    }

    public List<String> getQueryList() {
        return queryList;
    }

    public void setQueryList(List<String> queryList) {
        this.queryList = queryList;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getCep() {
        return cep;
    }

    public void setCep(String cep) {
        this.cep = cep;
    }

    public String getSiddhicep() {
        return siddhicep;
    }

    public void setSiddhicep(String siddhicep) {
        this.siddhicep = siddhicep;
    }

    public String getSiddhiSqlType() {
        return siddhiSqlType;
    }

    public void setSiddhiSqlType(String siddhiSqlType) {
        this.siddhiSqlType = siddhiSqlType;
    }

    public String getSiddhiSql() {
        return siddhiSql;
    }

    public void setSiddhiSql(String siddhiSql) {
        this.siddhiSql = siddhiSql;
    }

    public String getShellFile() {
        return shellFile;
    }

    public void setShellFile(String shellFile) {
        this.shellFile = shellFile;
    }

    public List<String> getThinks() {
        return thinks;
    }

    public void setThinks(List<String> thinks) {
        this.thinks = thinks;
    }

    public List<String> getAggCols() {
        return aggCols;
    }

    public void setAggCols(List<String> aggCols) {
        this.aggCols = aggCols;
    }

    public boolean isAgg() {
        return agg;
    }

    public void setAgg(boolean agg) {
        this.agg = agg;
    }

    public List<String> getJarList() {
        return jarList;
    }

    public void setJarList(List<String> jarList) {
        this.jarList = jarList;
    }

    public Map<String, String> getAttrs() {
        return attrs;
    }

    public void setAttrs(Map<String, String> attrs) {
        this.attrs = attrs;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public List<String> getSiddhiJoinStepsId() {
        return siddhiJoinStepsId;
    }

    public void setSiddhiJoinStepsId(List<String> siddhiJoinStepsId) {
        this.siddhiJoinStepsId = siddhiJoinStepsId;
    }

    public List<SiddhiFieldDTO> getSiddhiJoinFields() {
        return siddhiJoinFields;
    }

    public void setSiddhiJoinFields(List<SiddhiFieldDTO> siddhiJoinFields) {
        this.siddhiJoinFields = siddhiJoinFields;
    }

    public List<SiddhiJoinConditionDTO> getSiddhiJoinCondition() {
        return siddhiJoinCondition;
    }

    public void setSiddhiJoinCondition(List<SiddhiJoinConditionDTO> siddhiJoinCondition) {
        this.siddhiJoinCondition = siddhiJoinCondition;
    }
}
