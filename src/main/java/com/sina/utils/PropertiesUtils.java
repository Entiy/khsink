package com.sina.utils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by Administrator on 2017/6/26.
 */
public class PropertiesUtils {

    private static Properties properties;

    public static Properties load(String filePath){
        properties = new Properties();
        try {
            InputStream in =new BufferedInputStream(new FileInputStream(filePath));
            properties.load(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return properties;
    }

    public static String getString(String key){
        return properties.getProperty(key);
    }

}
