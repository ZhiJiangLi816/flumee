package com.bawei.sparkproject.test;

import com.bawei.sparkproject.conf.ConfigurationManager;

/***
 * @author Zhi-jiang li
 * @date 2020/1/31 0031 10:40
 **/
public class ConfigurationManagerTest {
    public static void main(String[] args) {
        String testkey1 = ConfigurationManager.getProperty("testkey1");
        String testkey2 = ConfigurationManager.getProperty("testkey2");

        System.out.println(testkey1+"n n "+testkey2);
    }
}
