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

    /**
     * 联邦集群的访问地址，同core-site.xml中的fs.defaultFS配置保持一致。
     * 且需要将core-site.xml和hdfs-site.xml拷贝到resources目录下，同时配置ip映射
     * tip: 通过该方式未能正确连接到集群上，需要后续再次确认具体的调用
     */
    public static final String FEDORATION_CLIENT = "viewfs://ClusterX";

    /**
     * 高可用主备集群的访问地址，同core-site.xml中的fs.defaultFS配置保持一致。
     * 且需要将core-site.xml和hdfs-site.xml拷贝到resources目录下，同时配置ip映射
     */
    public static final String HA_CLIENT = "hdfs://mycluster";


    public static final String HADOOP_USER = "root";

    static {
        String os = System.getProperty("os.name");
        if (StringUtils.containsIgnoreCase(os, "windows")) {
            NAMENODE_CLIENT = "hdfs://172.22.124.60:8020/";
        } else {
            NAMENODE_CLIENT = "hdfs://10.211.55.13:8020/";
        }
    }
}
