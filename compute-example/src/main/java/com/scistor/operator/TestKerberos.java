package com.scistor.operator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

public class TestKerberos {

    public static void main(String[] args) {
        Configuration conf = new Configuration(true);
        conf.set("hadoop.security.authentication", "Kerberos");
        System.setProperty("keytab", "C:\\Users\\chenlou\\Desktop\\fsdownload\\user.keytab");
        System.setProperty("java.security.krb5.conf", "C:\\Users\\chenlou\\Desktop\\fsdownload\\krb5.conf");
        UserGroupInformation.setConfiguration(conf);
        try {
            UserGroupInformation.loginUserFromKeytab("scistor@HADOOP.COM", System.getProperty("keytab"));
            System.out.println("认证成功");

//            FileSystem fs = FileSystem.newInstance(conf);
//            FileStatus fileStatus = fs.getFileStatus(new Path("/test/test.json"));
//            System.out.println(fileStatus);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
