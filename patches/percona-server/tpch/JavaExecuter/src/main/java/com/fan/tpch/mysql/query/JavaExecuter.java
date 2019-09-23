package com.fan.tpch.mysql.query;

import com.fan.tpch.mysql.query.util.MySQLUtil;
import com.fan.tpch.mysql.query.util.PropertiesUtil;
import com.fan.tpch.mysql.query.util.SQLFileReader;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

/**
 * Created by Administrator on 2018/3/30.
 */
public class JavaExecuter implements Runnable {

    private String querySQLBaseDir ="";

    public void intiConfig() throws Exception {
        File configFile = new File("config.properties");

        if (!configFile.exists()) {
            System.out.println("在当前目录下未找到配置文件，寻找备用配置文件！");
            configFile = new File("test-tools/config.properties");
            if (!configFile.exists()) {
                System.out.println("未能找到配置文件,退出！");
                throw new FileNotFoundException("未能找到配置文件,退出！");
            }
        }

        File queryBaseDir=new File("queries");
        querySQLBaseDir=queryBaseDir.getAbsolutePath()+"/";
        if(!queryBaseDir.exists()){
            System.out.println("在当前目录下未找到SQL文件夹queries，寻找备用文件夹！");
            queryBaseDir=new File("test-tools/queries");
            querySQLBaseDir=queryBaseDir.getAbsolutePath()+"/";
            if(!queryBaseDir.exists()){
                System.out.println("未能找到SQL文件夹queries,退出！");
                throw new FileNotFoundException("未能找到SQL文件夹queries件,退出！");
            }
        }

        Properties prop = PropertiesUtil.getProperties(configFile);
        String target_db_ip = prop.getProperty("target_db_ip");
        String target_db_port=prop.getProperty("target_db_port");
        String target_db_user=prop.getProperty("target_db_user");
        String target_db_password=prop.getProperty("target_db_password");
        String target_db_name=prop.getProperty("target_db_name");

        MySQLUtil.setUser(target_db_user);
        MySQLUtil.setPassword(target_db_password);
        // &useSSL=true
        String paramUrl="?autoReconnect=true&failOverReadOnly=false&socketTimeout=0&defaultStatementTimeout=30";
        MySQLUtil.setUrl("jdbc:mysql://"+target_db_ip+":"+target_db_port+"/"+target_db_name+paramUrl);
        MySQLUtil.testQuery();
    }

    public void run() {

        for (int i = 1; i <= 22; i++) {
//            if(i==5||i==15){continue;}
            long startTime = System.currentTimeMillis();
            Connection conn = null;
            Statement stmt = null;
            try {
                conn = MySQLUtil.getConnection();
                startTime = System.currentTimeMillis();
                System.out.println("第"+i+"条SQL开始执行"+System.currentTimeMillis());
                stmt = conn.createStatement();
                stmt.setQueryTimeout(100);
                String strSql = SQLFileReader.getText(querySQLBaseDir + i + ".sql");
                List<String> sqls = SQLFileReader.getSql(strSql);
                for (String sql : sqls) {
                    stmt.executeQuery(sql);
                }
                System.out.println("第"+i+"条执行时间：" + (System.currentTimeMillis() - startTime));
            } catch (Exception ex) {
                ex.printStackTrace();
                System.out.println("第"+i+"条执行失败时间："+(System.currentTimeMillis() - startTime));
                break;
            } finally {
                MySQLUtil.close(conn, null);

            }
        }

    }
}
