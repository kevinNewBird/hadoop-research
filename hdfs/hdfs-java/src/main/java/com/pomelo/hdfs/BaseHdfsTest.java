package com.pomelo.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
public abstract class BaseHdfsTest {

    private static final Logger logger = LoggerFactory.getLogger(BaseHdfsTest.class);

    private static FileSystem fs = null;

    protected abstract String url();

    @Before
    public void prepare() throws IOException, InterruptedException {
        // 1.创建conf对象
        Configuration conf = new Configuration(true);
        // 2.创建FileSystem对象
        fs = FileSystem.get(URI.create(url()), conf, HADOOP_USER);
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
     *
     * @throws IOException
     */
    @Test
    public void writeFileToHdfs() throws IOException {
        Path localFile = new Path(Objects.requireNonNull(BaseHdfsTest.class.getClassLoader()
                .getResource("Readme.md")).getFile().toString());

        Path remoteFile = new Path("/dir1/Readme.md");
        if (fs.exists(remoteFile)) {
            fs.delete(remoteFile, true);
        }

        fs.copyFromLocalFile(false, true, localFile, remoteFile);
        System.out.println("写入hdfs成功!!!");
    }

    /**
     * description: hdfs文件重命名
     * create by: zhaosong 2024/12/4 17:20
     *
     * @throws IOException
     */
    @Test
    public void renameHdfsFile() throws IOException {
        Path hdfsOldFile = new Path("/dir1/Readme.md");
        if (!fs.exists(hdfsOldFile)) {
            logger.warn("file[{}] not exists!", hdfsOldFile.getName());
            return;
        }

        Path hdfsNewFile = new Path("/dir1/Readme2.md");
        logger.info("file[{}] rename to  {}!", hdfsOldFile.getName(), hdfsNewFile.getName());
        fs.rename(hdfsOldFile, hdfsNewFile);
    }

    /**
     * description: 获取 hdfs文件或目录 的详细信息
     * create by: zhaosong 2024/12/4 17:21
     */
    @Test
    public void detailFromHdfsFile() throws IOException {
        Path filePath = new Path("/dir1/");
        if (!fs.exists(filePath)) {
            logger.warn("file[{}] not exists!", filePath.getName());
            return;
        }

        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(filePath, true);
        while (iterator.hasNext()) {
            LocatedFileStatus fileStatus = iterator.next();
            // 文件的详细信息
            System.out.println("文件详细信息如下：");
            System.out.println("文件路径：" + fileStatus.getPath());
            System.out.println("权限：" + fileStatus.getPermission());
            System.out.println("所有者：" + fileStatus.getOwner());
            System.out.println("所有者组：" + fileStatus.getGroup());
            System.out.println("大小：" + fileStatus.getLen());
            System.out.println("块大小：" + fileStatus.getBlockSize());
        }
    }

    /**
     * description:读取hdfs文件的内容
     * create by: zhaosong 2024/12/4 17:31
     */
    @Test
    public void readFromHdfsFile() throws IOException {
        Path hdfsFile = new Path("/dir1/Readme2.md");
        if (!fs.exists(hdfsFile)) {
            logger.warn("file[{}] not exists!", hdfsFile.getName());
            return;
        }
        try (FSDataInputStream fds = fs.open(hdfsFile);
             BufferedReader br = new BufferedReader(new InputStreamReader(fds))) {
            String line = "";
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
        }
    }

    /**
     * description: 删除文件或目录
     * create by: zhaosong 2024/12/4 17:36
     */
    @Test
    public void deleteHdfsFileOrDirectory() throws IOException {
        Path hdfsFile = new Path("/dir1/");
        if (!fs.exists(hdfsFile)) {
            logger.warn("file or directory [{}] not exists!", hdfsFile.getName());
            return;
        }
        // 递归删除
        boolean success = fs.delete(hdfsFile, true);
        System.out.println(success ? "delete success" : "failed to delete");
    }
}
