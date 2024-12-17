package com.pomelo.joiner;

import com.pomelo.joiner.writable.StaffWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
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
public class StaffDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1.创建配置及job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2.设置Driver驱动类
        job.setJarByClass(StaffDriver.class);

        // 3.设置mapper、reducer对应的类
        MultipleInputs.addInputPath(job, new Path("data/person.txt"), TextInputFormat.class, PersonMapper.class);
        MultipleInputs.addInputPath(job, new Path("data/address.txt"), TextInputFormat.class, AddressMapper.class);
        job.setReducerClass(StaffReducer.class);

        // 4.设置mapper输出的K,V类型
        job.setMapOutputKeyClass(StaffWritable.class);
        job.setMapOutputValueClass(StaffWritable.class);

        // 5.设置最终输出数据的K,V类型
        job.setOutputKeyClass(StaffWritable.class);
        job.setOutputValueClass(NullWritable.class);

        // 6.设置数据输出的路径
        FileOutputFormat.setOutputPath(job, new Path("data/output"));

        // 7.运行任务
        boolean success = job.waitForCompletion(true);
        System.out.println(success ? "任务执行成功...." : "任务执行失败！！！");
    }
}
