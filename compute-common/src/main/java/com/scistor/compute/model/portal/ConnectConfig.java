package com.scistor.compute.model.portal;

import java.io.Serializable;
import java.util.Map;

public class ConnectConfig implements Serializable {
    private String connection_url;
    private String user_name;
    private String password;
    private Map<String,String> parameters;

    public ConnectConfig(String connection_url, String user_name, String password) {
        this.connection_url = connection_url;
        this.user_name = user_name;
        this.password = password;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    public String getConnection_url() {
        return connection_url;
    }

    public void setConnection_url(String connection_url) {
        this.connection_url = connection_url;
    }

    public String getUser_name() {
        return user_name;
    }

    public void setUser_name(String user_name) {
        this.user_name = user_name;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
