package com.pomelo.formatter.format;

import com.pomelo.formatter.writable.ScoreWritable;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * description: com.pomelo.formatter.format
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/12/17
 * version: 1.0
 */
public class ScoreOutputFormat extends FileOutputFormat<ScoreWritable, NullWritable> {

    @Override
    public RecordWriter<ScoreWritable, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {

        return new StudentRecordWriter(context);
    }

    /**
     * 实现将Reducer输出K,V写出
     */
    static class StudentRecordWriter extends RecordWriter<ScoreWritable, NullWritable> {
        FSDataOutputStream passOs;
        FSDataOutputStream failOs;

        public StudentRecordWriter(TaskAttemptContext context) throws IOException {
            // 创建FileSystem
            FileSystem fs = FileSystem.get(context.getConfiguration());
            // 创建输出流 - 针对写出的不同文件都要创建： pass.txt, fail.txt
            passOs = fs.create(new Path("data/output/pass.txt"));
            failOs = fs.create(new Path("data/output/fail.txt"));
        }

        /**
         * 将数据写出到文件
         *
         * @param key   the key to write.
         * @param value the value to write.
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void write(ScoreWritable key, NullWritable value) throws IOException, InterruptedException {
            int score = key.getScore();
            if (score >= 80) {
//                passOs.writeBytes(score + "\n");
                passOs.writeBytes(key + "\n");
            } else {
//                failOs.writeBytes(score + "\n");
                failOs.writeBytes(key + "\n");
            }
        }

        /**
         * 关闭资源
         *
         * @param context the context of the task
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            // 关闭流对象
            IOUtils.closeStreams(passOs, failOs);
        }
    }
}
