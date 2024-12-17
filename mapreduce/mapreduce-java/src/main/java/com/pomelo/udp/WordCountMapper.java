package com.pomelo.udp;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * description: com.pomelo.wordcount
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/12/16
 * version: 1.0
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text returnKey = new Text();

    IntWritable returnVal = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value
            , Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        // 数据格式： hello zhangsan
        String line = value.toString(); // 一行数据

        // 切分数据
        String[] words = StringUtils.split(line, " ");

        // 写出数据
        for (String word : words) {
            returnKey.set(word);
            context.write(returnKey, returnVal);
        }
    }
}
