package com.scistor.compute.model.spark;

import java.io.Serializable;

public class UDFConfig implements Serializable {

    public String udfName;
    public String udfClassFullName;
    public String jarPath;
    public String mapOrAgg = "udf";

    public UDFConfig(){

    }

    public String getUdfName() {
        return udfName;
    }

    public void setUdfName(String udfName) {
        this.udfName = udfName;
    }

    public String getUdfClassFullName() {
        return udfClassFullName;
    }

    public void setUdfClassFullName(String udfClassFullName) {
        this.udfClassFullName = udfClassFullName;
    }

    public String getJarPath() {
        return jarPath;
    }

    public void setJarPath(String jarPath) {
        this.jarPath = jarPath;
    }

    public String getMapOrAgg() {
        return mapOrAgg;
    }

    public void setMapOrAgg(String mapOrAgg) {
        this.mapOrAgg = mapOrAgg;
    }
}
