package com.pomelo.model.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathOperationException;

import java.io.IOException;
import java.net.URI;

/**
 *
 */
public class FinkFileSourceReadHdfsTest {

    private static final String HA_HDFS_URL = "hdfs://mycluster";

    private static final String DATA_DIR = "/flink-java/data-source/";

    private static final String DATA_FILE = "flink-java.txt";

    private static final String HDFS_DATA_FILE_PATH = HA_HDFS_URL + DATA_DIR + DATA_FILE;

    /**
     * 用途：读取文件数据（本地文件或者hdfs）
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // 1.准备数据
        createFileWithHdfs();

        // 2.准备flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 3.读取hdfs文件（data source）,即数据源的方式
        // flink 1.14版本之前读取文件的写法：DataStreamSource<String> dataStream = env.readTextFile("hdfs://mycluster/flinkdata/data.txt");
        FileSource<String> datasource = FileSource.forRecordStreamFormat(
                        new TextLineInputFormat(),
                        new org.apache.flink.core.fs.Path(HDFS_DATA_FILE_PATH))
                .build();

        // 4.flink加载数据源做作处理
        DataStreamSource<String> datastream = env.fromSource(datasource, WatermarkStrategy.noWatermarks(), "file-source");

        // 5.data sink
        datastream.print();

        // 6.执行
        env.execute();
    }


    private static void createFileWithHdfs() throws IOException, InterruptedException {
        // 1.创建conf对象
        Configuration conf = new Configuration(true);
        // 2.创建FileSystem对象
        FileSystem fs = FileSystem.get(URI.create(HA_HDFS_URL), conf, "root");

        // 3.创建目录
        if (fs.exists(new Path(DATA_DIR))) {
            fs.delete(new Path(DATA_DIR), true);
        }
        boolean isSuccess = fs.mkdirs(new Path(DATA_DIR));
        if (!isSuccess) {
            throw new PathOperationException("failed to create new directory: " + DATA_DIR);
        }

        // 4.上传本地数据到hdfs
        Path localDataPath = new Path(FinkFileSourceReadHdfsTest.class.getClassLoader().getResource("flink-java.txt").getFile().toString());
        Path remoteDatePath = new Path(DATA_DIR, "flink-java.txt");
        if (fs.exists(remoteDatePath)) {
            fs.delete(remoteDatePath, true);
        }

        // 5.上传
        fs.copyFromLocalFile(false, true, localDataPath, remoteDatePath);
    }
}
