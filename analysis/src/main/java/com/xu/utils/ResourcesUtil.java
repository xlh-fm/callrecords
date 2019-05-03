package com.xu.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ResourcesUtil {
    private static Properties properties = null;

    public static Properties getProperties(){
        InputStream resourceAsStream = ClassLoader.getSystemResourceAsStream("kafka.properties");
        properties = new Properties();
        try {
            properties.load(resourceAsStream);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return properties;
    }
}