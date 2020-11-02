package com.scistor.compute.model.remote;

import java.io.Serializable;

public class RedisConfig implements Serializable {
    private String host;
    private int port;
    private String password;
    private int database = 0;

    public RedisConfig(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public RedisConfig(String host, int port, String password) {
        this.host = host;
        this.port = port;
        this.password = password;
    }

    public RedisConfig(String host, int port, String password, int database) {
        this.host = host;
        this.port = port;
        this.password = password;
        this.database = database;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getDatabase() {
        return database;
    }

    public void setDatabase(int database) {
        this.database = database;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"host\":\"")
                .append(host).append('\"');
        sb.append(",\"port\":")
                .append(port);
        sb.append(",\"password\":\"")
                .append(password).append('\"');
        sb.append('}');
        return sb.toString();
    }
}
