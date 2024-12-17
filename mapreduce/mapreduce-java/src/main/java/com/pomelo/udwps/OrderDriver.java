package com.pomelo.udwps;

import com.pomelo.udwps.partitioner.OrderPartitioner;
import com.pomelo.udwps.writable.OrderWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * description: com.pomelo.udws
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/12/17
 * version: 1.0
 */
public class OrderDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1.创建配置及job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2.设置Driver驱动类
        job.setJarByClass(OrderDriver.class);

        // 3.设置mapper、reducer对应的类
        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);

        // 4.设置mapper输出的K,V类型
        job.setMapOutputKeyClass(OrderWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 5.设置最终输出数据的K,V类型
        job.setOutputKeyClass(OrderWritable.class);
        job.setOutputValueClass(NullWritable.class);

        // 6.设置分区器
        job.setPartitionerClass(OrderPartitioner.class);
        job.setNumReduceTasks(3);

        // 7.设置数据输入和输出的路径
        FileInputFormat.setInputPaths(job, new Path("data/order.txt"));
        FileOutputFormat.setOutputPath(job, new Path("data/output"));

        // 8.运行任务
        boolean success = job.waitForCompletion(true); // true表示打印中间的结果，false表示不打印中间的结果
        System.out.println(success ? "任务执行成功...." : "任务执行失败！！！");
    }
}
