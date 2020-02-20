package com.bawei.sparkproject.conf;

import java.io.InputStream;
import java.util.Properties;

/***
 * @author Zhi-jiang li
 * @date 2020/1/31 0031 10:30
 **/
public class ConfigurationManager {
    private static Properties properties = new Properties();

    /**
     * 静态代码块
     *
     * Java中每一个类的第一次使用,就会被Java虚拟机(JVM)中的加载器,去从磁盘上的.class文件
     * 加载出来,然后为每个类构建一个class对象,就代表了这个类
     */
    static {
        try {
            InputStream inputStream = ConfigurationManager.class
                    .getClassLoader().getResourceAsStream("my.properties");

            properties.load(inputStream);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 通过Key的到相对应的value的值
     * @param key
     * @return
     */
    public static String getProperty(String key){
        return properties.getProperty(key);
    }

    /**
     * 获取整数类型的配置项
     * @param key
     * @return
     */
    public static Integer getInteger(String key){
        String value = ConfigurationManager.getProperty(key);
        try {
            return Integer.valueOf(value);
        }catch (Exception e){
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * 获取布尔类型的配置项
     * @param key
     * @return
     */
    public static Boolean getBoolean(String key){
        String value = ConfigurationManager.getProperty(key);
        try {
            return Boolean.valueOf(value);
        }catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }

    public static Long getLong(String key) {
        String value = ConfigurationManager.getProperty(key);
        try {
            return Long.valueOf(value);
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
}
