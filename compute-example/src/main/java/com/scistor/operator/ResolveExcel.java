package com.scistor.operator;

import com.scistor.compute.interfacex.DataSourceMoreInterface;

import java.util.*;

public class ResolveExcel implements DataSourceMoreInterface {

    @Override
    public List<Map<String, Object>> process(Map<String, Object> rowdata) {
        List<Map<String, Object>> res = new ArrayList<>();
        if (rowdata.containsKey("sip")) {
            String sip = rowdata.getOrDefault("sip", "").toString();
            if (sip != "" && sip.indexOf("/") >= 0) {
                String[] split = sip.split("\\.");
                if (split.length >= 4 && split[3].indexOf("/") >= 0) {
                    String ip = split[0] + "." + split[1] + "." + split[2] + "." + split[3].split("/")[0];
                    String maskBit = split[3].split("/")[1];
                    List<Long> ips = IPChange.parseIpMaskRange(ip, maskBit);
                    for (Long i: ips) {
                        Map<String, Object> temp = new HashMap<>();
                        temp.putAll(rowdata);
                        temp.put("sip", i);
                        res.add(temp);
                    }
                }
            } else {
                Long ipFromString = IPChange.getIpFromString(sip);
                Map<String, Object> temp = new HashMap<>();
                temp.putAll(rowdata);
                temp.put("sip", ipFromString);
                res.add(temp);
            }
        }
        return res;
    }

    public static void main(String[] args) {
        Map<String, Object> map = new HashMap<>();
        map.put("sip", "10.10.10.0/24");
        map.put("dip", "10.10.10.1");
        ResolveExcel resolveExcel = new ResolveExcel();
        List<Map<String, Object>> process = resolveExcel.process(map);
        System.out.println(process.size());
        for (Map<String, Object> m : process) {
            System.out.println(m.toString());
        }
    }

}
