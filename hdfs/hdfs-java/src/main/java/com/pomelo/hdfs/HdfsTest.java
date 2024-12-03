package com.pomelo.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

import static com.pomelo.hdfs.constant.HdfsConstant.HADOOP_USER;
import static com.pomelo.hdfs.constant.HdfsConstant.NAMENODE_CLIENT;


/**
 * description: com.pomelo.hdfs
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/12/3
 * version: 1.0
 */
public class HdfsTest {

    private static FileSystem fs = null;

    @Before
    public void prepare() throws IOException, InterruptedException {
        Configuration conf = new Configuration(true);
        // 创建FileSystem对象
        fs = FileSystem.get(URI.create(NAMENODE_CLIENT), conf, HADOOP_USER);
    }

    /**
     * description: 查看HDFS路径文件
     * create by: zhaosong 2024/12/3 19:00
     *
     * @throws IOException
     */
    @Test
    public void listHdfsPathDir() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            System.out.println(fileStatus.getPath());
        }
    }

}
