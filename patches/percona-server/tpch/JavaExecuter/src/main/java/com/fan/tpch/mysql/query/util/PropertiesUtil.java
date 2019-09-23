package com.fan.tpch.mysql.query.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by Administrator on 2018/3/30.
 */
public class PropertiesUtil {

    public static Properties getProperties(File configFile) throws Exception {

        if (!configFile.exists()) {
            throw new FileNotFoundException("配置文件未找到");
        }

        InputStream in = null;
        Properties pro = new Properties();

        try {
            in = new FileInputStream(configFile);
            pro.load(in);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
        return pro;
    }
}
