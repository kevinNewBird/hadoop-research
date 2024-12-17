package com.pomelo.group.partitioner;

import com.pomelo.group.writable.TemperatureWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.StringJoiner;

/**
 * description: com.pomelo.group.partitioner
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/12/17
 * version: 1.0
 */
public class TemperaturePartitioner extends Partitioner<TemperatureWritable, TemperatureWritable> {

    @Override
    public int getPartition(TemperatureWritable key, TemperatureWritable value, int numPartitions) {
        StringJoiner joiner = new StringJoiner("-", key.getYear(), key.getMonth());
        return (joiner.toString().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
