package com.pomelo.mapjoiner;

import com.pomelo.joiner.writable.StaffWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
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
public class MapJoinDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1.创建配置及job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2.设置Driver驱动类
        job.setJarByClass(MapJoinDriver.class);

        // 3.设置mapper对应的类
        job.setMapperClass(MapJoinMapper.class);

        // 4.设置mapper输出的K,V类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 5.设置最终输出数据的K,V类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 6.设置缓存文件（可以添加多次）
        job.addCacheFile(new Path("data/address.txt").toUri());

        // 7.设置数据输出的路径
        FileInputFormat.setInputPaths(job, new Path("data/person.txt"));
        FileOutputFormat.setOutputPath(job, new Path("data/output"));

        // 8.运行任务
        boolean success = job.waitForCompletion(true);
        System.out.println(success ? "任务执行成功...." : "任务执行失败！！！");
    }
}
