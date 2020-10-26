package com.scistor.compute.interfacex;

import java.util.List;
import java.util.Map;

public interface DataSourceMoreInterface extends DataSourceInterface {
    List<Map<String, Object>> process(Map<String, Object> rowdata);
}
