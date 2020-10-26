package com.scistor.compute.model.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class SourceSchme implements Serializable {

    public String name;
    public String type;
    public boolean nullable;
    public String rename;
    public List<SourceSchme> sub_fields = new ArrayList<>();

    SourceSchme(){

    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public String getRename() {
        return rename;
    }

    public void setRename(String rename) {
        this.rename = rename;
    }

    public List<SourceSchme> getSub_fields() {
        return sub_fields;
    }

    public void setSub_fields(List<SourceSchme> sub_fields) {
        this.sub_fields = sub_fields;
    }
}
