package com.pomelo.hdfs.constant;

import org.apache.commons.lang3.StringUtils;

/**
 * description: com.pomelo.hdfs.constant
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/12/3
 * version: 1.0
 */
public final class HdfsConstant {

    public static final String NAMENODE_CLIENT;

    public static final String HADOOP_USER = "root";

    static {
        String os = System.getProperty("os.name");
        if (StringUtils.containsIgnoreCase(os, "windows")) {
            NAMENODE_CLIENT = "hdfs://172.22.124.60:8020/";
        } else {
            NAMENODE_CLIENT = "hdfs://";
        }
    }
}
