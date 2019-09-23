package com.fan.tpch.mysql.query.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * Created by Administrator on 2018/3/30.
 */
public class MySQLUtil {

    // 驱动名称
    private static String driver = "com.mysql.jdbc.Driver";
    // URL指向要访问的数据库名scutcs
    private static String url = "jdbc:mysql://172.18.100.62:4000/tpch";
    // MySQL配置时的用户名
    private static String user = "root";
    // Java连接MySQL配置时的密码
    private static String password = "";

    public static String getUrl() {
        return url;
    }

    public static void setUrl(String url) {
        MySQLUtil.url = url;
    }

    public static String getUser() {
        return user;
    }

    public static void setUser(String user) {
        MySQLUtil.user = user;
    }

    public static String getPassword() {
        return password;
    }

    public static void setPassword(String password) {
        MySQLUtil.password = password;
    }

    public static void testQuery() throws Exception{
            Connection conn = getConnection();
            if(conn==null){
                throw new Exception("测试查询异常，请检查配置");
            }
            Statement stmt = conn.createStatement();
            stmt.execute("select 1");
            close(conn, stmt);

    }

    public static Connection getConnection() {


        Connection conn = null;
        try {
            // 加载驱动程序
            Class.forName(driver);
            // 连接数据库
            conn = DriverManager.getConnection(url, user, password);
            if (conn.isClosed()) {
                return null;
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return conn;
    }

    public static void close(Connection conn, Statement stmt) {

        if (stmt != null) {
            try {
                stmt.close();
            } catch (Exception ex) {
            } finally {
                stmt = null;
            }
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (Exception ex) {

            } finally {
                conn = null;
            }
        }
    }
}
