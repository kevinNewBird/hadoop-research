package com.pomelo.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Objects;

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
        // 1.创建conf对象
        Configuration conf = new Configuration(true);
        // 2.创建FileSystem对象
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
        listHdfsPathDir("/");
    }

    /**
     * 查看HDFS路径文件
     *
     * @param path
     * @throws IOException
     */
    private void listHdfsPathDir(String path) throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path(path));
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isDirectory()) {
                listHdfsPathDir(fileStatus.getPath().toString());
            } else {
                System.out.println(fileStatus.getPath());
            }
        }
    }


    /**
     * 创建目录
     */
    @Test
    public void createHdfsDirectory() throws IOException {
        Path path = new Path("/dir1");
        // 1.判断目录是否存在
        if (fs.exists(path)) {
            System.err.println("directory exists!!!");
            return;
        }
        // 2.创建目录
        boolean success = fs.mkdirs(path);
        System.out.println(success ? "create successfully" : "failed to create");
    }

    /**
     * 写入文件数据到hdfs
     * @throws IOException
     */
    @Test
    public void writeFileToHdfs() throws IOException {
        Path localFile = new Path(Objects.requireNonNull(HdfsTest.class.getClassLoader()
                .getResource("Readme.md")).getFile().toString());

        Path remoteFile = new Path("/dir1/Readme.md");
        if (fs.exists(remoteFile)) {
            fs.delete(remoteFile, true);
        }

        fs.copyFromLocalFile(false, true, localFile, remoteFile);
        System.out.println("写入hdfs成功!!!");
    }
}
