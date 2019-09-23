package com.fan.tpch.mysql;

import com.fan.tpch.mysql.query.JavaExecuter;

/**
 * Created by Administrator on 2018/4/2.
 */
public class App {

    public static void main(String[] args) throws Exception {
        JavaExecuter javaExecuter = new JavaExecuter();
        javaExecuter.intiConfig();

        Thread t=new Thread(javaExecuter);
        t.setDaemon(false);
        t.start();
    }
}
