package com.pomelo.udw;

import com.pomelo.udw.writable.CarWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * description: com.pomelo.uds
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/12/17
 * version: 1.0
 */
public class CarAnalysisDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1.创建配置及job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2.设置Driver驱动类
        job.setJarByClass(CarAnalysisDriver.class);

        // 3.设置mapper、reducer对应的类
        job.setMapperClass(CarAnalysisMapper.class);
        job.setReducerClass(CarAnalysisReducer.class);

        // 4.设置mapper输出的K,V类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CarWritable.class);

        // 5.设置最终输出数据的K,V类型
        job.setOutputKeyClass(Text.class);
        // (不输出key)
//        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(CarWritable.class);

        // 6.设置数据输入和输出的路径
        FileInputFormat.setInputPaths(job, new Path("data/car.txt"));
        FileOutputFormat.setOutputPath(job, new Path("data/output"));

        // 7.运行任务
        boolean success = job.waitForCompletion(true); // true表示打印中间的结果，false表示不打印中间的结果
        System.out.println(success ? "任务执行成功...." : "任务执行失败！！！");
    }
}
