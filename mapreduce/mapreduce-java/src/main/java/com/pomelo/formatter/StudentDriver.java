package com.pomelo.formatter;

import com.pomelo.formatter.format.StudentOutputFormat;
import com.pomelo.formatter.writable.StudentWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * description: com.pomelo.formatter
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/12/17
 * version: 1.0
 */
public class StudentDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1.创建配置及job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2.设置Driver驱动类
        job.setJarByClass(StudentDriver.class);

        // 3.设置mapper、reducer对应的类
        job.setMapperClass(StudentMapper.class);
        job.setReducerClass(StudentReducer.class);

        // 4.设置mapper输出的K,V类型
        job.setMapOutputKeyClass(StudentWritable.class);
        job.setMapOutputValueClass(Text.class);

        // 5.设置最终输出数据的K,V类型
        job.setOutputKeyClass(StudentWritable.class);
        job.setOutputValueClass(NullWritable.class);

        // 6.设置自定义格式器
        job.setOutputFormatClass(StudentOutputFormat.class);

        // 7.设置数据输入和输出的路径
        FileInputFormat.setInputPaths(job, new Path("data/student.txt"));
        // 将写出数据成功的标志文件写出到该目录
        FileOutputFormat.setOutputPath(job, new Path("data/output"));

        // 8.运行任务
        boolean success = job.waitForCompletion(true);
        System.out.println(success ? "任务执行成功...." : "任务执行失败！！！");
    }
}
