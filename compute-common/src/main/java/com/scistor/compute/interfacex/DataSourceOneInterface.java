package com.scistor.compute.interfacex;

import java.util.Map;

public interface DataSourceOneInterface extends DataSourceInterface {
    Map<String, Object> process(Map<String, Object> rowdata);
}
