package com.pomelo.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * description: mapreduce任务的驱动类
 * <tip/>
 * 可以配置任务执行的参数：Mapper、Reducer、分区、分组等相关信息
 * <p>
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/12/16
 * version: 1.0
 */
public class WordCountDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1.创建配置及job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2.设置Driver驱动类
        job.setJarByClass(WordCountDriver.class);

        // 3.设置mapper、reducer对应的类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4.设置mapper输出的K,V类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5.设置最终输出数据的K,V类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置分区数（默认为1.即设置reduce的分区数量，提高并发数）
        job.setNumReduceTasks(2); // 最终会有两个文件输出

        // 6.设置数据输入和输出的路径
        FileInputFormat.setInputPaths(job, new Path("data/data.txt"));
        FileOutputFormat.setOutputPath(job, new Path("data/output"));

        // 7.运行任务
        boolean success = job.waitForCompletion(true);
        System.out.println(success ? "任务执行成功...." : "任务执行失败！！！");
    }
}
