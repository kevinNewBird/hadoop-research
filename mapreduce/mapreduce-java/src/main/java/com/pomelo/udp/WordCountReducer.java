package com.pomelo.udp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * description: com.pomelo.wordcount
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/12/16
 * version: 1.0
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    IntWritable returnTotal = new IntWritable(); // 避免大量的内存占用，所有的key共享该对象

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values
            , Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // 记录当前key的总数，即value值
        int sum = 0;

        for (IntWritable value : values) {
            sum += value.get();
        }

        // 写出数据
        returnTotal.set(sum);
        context.write(key, returnTotal);
    }
}
