package com.scistor.operator;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {

    public java.util.Map<String, String> process(String... args) {
        java.util.Map<String, String> map = new java.util.HashMap<String, String>();
        map.put("sip_out", args[0]+"_aaaaaaaaaa");
        map.put("dip_out", args[1]+"_bbbbbbbbbb");
        return map;
    }

    public static void main(String[] args) {
//        String str = "10.10.10.10.10";
//        java.util.regex.Pattern compile = java.util.regex.Pattern.compile("10.10.10.10");
//        java.util.regex.Matcher matcher = compile.matcher(str);
//        boolean res = matcher.matches();
//        System.out.println(res);

//        long checksum = 10;
//
//        long n2 = (checksum >> 16) + (checksum & 0xffff) * 2;
//
//        System.out.println(n2);

    }

}
