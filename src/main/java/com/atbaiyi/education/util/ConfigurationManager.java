package com.atbaiyi.education.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 *
 * 读取配置文件工具类
 */
public class ConfigurationManager {
    private  static Properties prop=new Properties();

    static {

        InputStream inputStream = ConfigurationManager.class.getClassLoader()
                .getResourceAsStream("comerce.properties");
        try {
            prop.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    //获取配置项
    public static String getProperty(String key) {
        return prop.getProperty(key);
    }

    //获取布尔类型的配置项
    public static boolean getBoolean(String key) {
        String value = prop.getProperty(key);
        try {
            return Boolean.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

}



