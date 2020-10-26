package com.scistor.compute.interfacex;

import java.io.Serializable;
import java.util.Map;

public interface DataSourceInterface extends Serializable {
    final static String KAFKATOPIC = "KAFKATOPIC";
    final static String KAFKAMSGKEY = "KAFKAMSGKEY";
    final static String KAFKAMSGVAL = "KAFKAMSGVAL";
    final static String KAFKAMSGOFFSET = "KAFKAMSGOFFSET";
    final static String KAFKAPARTITION = "KAFKAPARTITION";
    final static String KAFKATIMRSTAMP = "KAFKATIMRSTAMP";

    final static String HDFSFILEPATH = "HDFSFILEPATH";
    final static String HDFSFILEDATA = "HDFSFILEDATA";

    default String getKAFKATOPIC(Map<String, Object> rowdata){
        return (String) rowdata.get(KAFKATOPIC);
    }

    default String getKAFKAMSGKEY(Map<String, Object> rowdata){
        return (String) rowdata.get(KAFKAMSGKEY);
    }

    default byte[] getKAFKAMSGVAL(Map<String, Object> rowdata){
        return (byte[]) rowdata.get(KAFKAMSGVAL);
    }

    default long getKAFKAMSGOFFSET(Map<String, Object> rowdata){
        return (long) rowdata.get(KAFKAMSGOFFSET);
    }

    default int getKAFKAPARTITION(Map<String, Object> rowdata){
        return (int) rowdata.get(KAFKAPARTITION);
    }

    default long getKAFKATIMRSTAMP(Map<String, Object> rowdata){
        return (long) rowdata.get(KAFKATIMRSTAMP);
    }

    default String getHDFSFILEPATH(Map<String, Object> rowdata){
        return (String) rowdata.get(HDFSFILEPATH);
    }

    default byte[] getHDFSFILEDATA(Map<String, Object> rowdata){
        return (byte[]) rowdata.get(HDFSFILEDATA);
    }
}
