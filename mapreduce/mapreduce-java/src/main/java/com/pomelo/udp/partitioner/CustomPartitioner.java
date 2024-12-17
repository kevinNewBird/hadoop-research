package com.pomelo.udp.partitioner;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * description: 自定义分区器
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/12/17
 * version: 1.0
 */
public class CustomPartitioner extends Partitioner<Text, IntWritable> {

    @Override
    public int getPartition(Text text, IntWritable intWritable, int numPartitions) {
        // （！！！）注意： 分区器里面的分区数必须要小于等于Driver里面的setNumReduceTasks设置，否则会报错
        String key = text.toString();
        if (StringUtils.equalsIgnoreCase("zhangsan", key)
                || StringUtils.equalsIgnoreCase("lisi", key)) {
            return 0;
        }
        return 1;
    }
}