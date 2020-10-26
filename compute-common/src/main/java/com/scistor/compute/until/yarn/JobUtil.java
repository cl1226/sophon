package com.scistor.compute.until.yarn;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JobUtil {

    private Logger logger = Logger.getLogger(this.getClass().getName());

    private Properties prop = new Properties();

    private ExecuteShellUtil executeShellUtil = ExecuteShellUtil.getInstance();

    public static void main(String[] args) {
        String jobName = "写入postgresql模型_admin_0edd206c8a124aa285d7126cce1865f9";
        JobUtil jobUtil = new JobUtil();

        String jobStatus = "";
        String jobErrLog = "";
        try {
            jobStatus = jobUtil.getJobStatus(jobName);
            jobErrLog = jobUtil.getJobErrLog(jobName);
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("jobStatus = " + jobStatus);
        System.out.println("jobErrLog = " + jobErrLog);
    }

    public JobUtil() {
        try {
            InputStream in = this.getClass().getResourceAsStream("/yarn_cmd.properties");
            prop.load(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public JobUtil(String confPath) {
        try {
            InputStream in = new FileInputStream(new File(confPath));
            prop.load(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * get application_id by jobName
     */
    public String getApplicationId(String jobName) throws Exception {
        String application_id = "";
        String cmd = prop.getProperty("get_job_id").replaceAll("jobName", jobName);
        logger.info(" getJobId cmd : " + cmd);

        executeShellUtil.init("192.168.31.77", 22, "root", "123456");
        String res = executeShellUtil.execCmd(cmd);

        Pattern p = Pattern.compile(prop.getProperty("get_job_id_reg"));
        Matcher matcher = p.matcher(res);
        if(matcher.find()) {
            application_id = matcher.group(Integer.parseInt(prop.getProperty("get_job_id_reg_group")));
        }

        System.out.println("applicationId: " + application_id);
        return application_id;
    }

    /**
     * get job status by jobName
     */
    public String getJobStatus(String jobName) throws Exception {
        String jobStatus = "";
        String application_id = getApplicationId(jobName);
        String cmd = prop.getProperty("get_job_status").replaceAll("application_id", application_id);
        logger.info(" getJobStatus cmd : " + cmd);
        System.out.println("getJobStatus cmd: " + cmd);

        executeShellUtil.init("192.168.31.77", 22, "root", "123456");
        String res = executeShellUtil.execCmd(cmd);

        Pattern p = Pattern.compile(prop.getProperty("get_job_status_reg"));
        Matcher matcher = p.matcher(res);
        if(matcher.find()) {
            jobStatus=matcher.group(Integer.parseInt(prop.getProperty("get_job_status_reg_group")));
        }
        return jobStatus;
    }

    /**
     * get job stderr log
     */
    public String getJobErrLog(String jobName) throws Exception {
        String application_id = getApplicationId(jobName);
        String cmd = prop.getProperty("get_job_error_log").replaceAll("application_id", application_id);
        logger.info("getJobErrLog cmd : " + cmd);
        System.out.println("getJobErrLog cmd: " + cmd);

        executeShellUtil.init("192.168.31.77", 22, "root", "123456");
        String res = executeShellUtil.execCmd(cmd);

        return res;
    }

}
