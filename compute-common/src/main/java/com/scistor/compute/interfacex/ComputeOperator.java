package com.scistor.compute.interfacex;

import java.io.Serializable;
import java.util.Map;

public interface ComputeOperator extends Serializable {
    String STEP_NAME = "STEP_NAME";
    String MODULE_NAME = "MODULE_NAME";
    String JOB_NAME = "JOB_NAME";
    String OPERATOR_TYPE = "OPERATOR_TYPE";
    String PRIVATE_XMLPATH = "PRIVATE_XMLPATH";

    Map<String,Object> process(Map<String,Object> rowdata, Map<String,String> attributes);
}
