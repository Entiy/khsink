package com.sina.utils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by Administrator on 2017/6/26.
 */
public class PropertiesUtils {

    public static Properties properties=null;
    private static final String FILEPATH="/khsink.properties";

    public static void load(String filePath) {

        properties=new Properties();
        InputStream inputStream = PropertiesUtils.class.getResourceAsStream(filePath);
        try {
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void load() {

        properties=new Properties();
        InputStream inputStream = PropertiesUtils.class.getResourceAsStream(FILEPATH);
        try {
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static String getString(String key){
        return properties.getProperty(key);
    }

}
