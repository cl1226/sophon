package com.scistor.compute.model.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ComputeJob implements Serializable {

    private String projectName;
    private String jobName;

    private String jobId;
    private String dataSource;
    private OperatorType operatorType;

    private SourceAttribute attribute = new SourceAttribute();
    private SinkAttribute sinkAttribute;

    private String siddhi_sql;

    private List<InvokeInfo> process = new ArrayList<>();

    private List<String> processSql = new ArrayList<>();

    private List<UserDefineOperator> udos = new ArrayList<>();

    private Map<String, List<String>> command = new HashMap<>();

    private Map<String, String> commandMaping = new HashMap<>();

    private List<String> flinkSql = new ArrayList<>();

    private Map<String, String> attrs = new HashMap<>();
    //agg
    private boolean isAgg = false;

    private List<String> aggCols = new ArrayList<>();

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getDataSource() {
        return dataSource;
    }

    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    public OperatorType getOperatorType() {
        return operatorType;
    }

    public void setOperatorType(OperatorType operatorType) {
        this.operatorType = operatorType;
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

    public String getSiddhi_sql() {
        return siddhi_sql;
    }

    public void setSiddhi_sql(String siddhi_sql) {
        this.siddhi_sql = siddhi_sql;
    }

    public List<InvokeInfo> getProcess() {
        return process;
    }

    public void setProcess(List<InvokeInfo> process) {
        this.process = process;
    }

    public List<String> getProcessSql() {
        return processSql;
    }

    public void setProcessSql(List<String> processSql) {
        this.processSql = processSql;
    }

    public List<UserDefineOperator> getUdos() {
        return udos;
    }

    public void setUdos(List<UserDefineOperator> udos) {
        this.udos = udos;
    }

    public Map<String, List<String>> getCommand() {
        return command;
    }

    public void setCommand(Map<String, List<String>> command) {
        this.command = command;
    }

    public Map<String, String> getCommandMaping() {
        return commandMaping;
    }

    public void setCommandMaping(Map<String, String> commandMaping) {
        this.commandMaping = commandMaping;
    }

    public List<String> getFlinkSql() {
        return flinkSql;
    }

    public void setFlinkSql(List<String> flinkSql) {
        this.flinkSql = flinkSql;
    }

    public Map<String, String> getAttrs() {
        return attrs;
    }

    public void setAttrs(Map<String, String> attrs) {
        this.attrs = attrs;
    }

    public boolean isAgg() {
        return isAgg;
    }

    public void setAgg(boolean agg) {
        isAgg = agg;
    }

    public List<String> getAggCols() {
        return aggCols;
    }

    public void setAggCols(List<String> aggCols) {
        this.aggCols = aggCols;
    }
}
