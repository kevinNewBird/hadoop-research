package com.pomelo.udps.partitioner;

import com.pomelo.udps.writable.OrderWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * description: com.pomelo.udps.partitioner
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/12/17
 * version: 1.0
 */
public class OrderPartitioner extends Partitioner<OrderWritable, NullWritable> {

    @Override
    public int getPartition(OrderWritable orderWritable, NullWritable nullWritable, int numPartitions) {
        String time = orderWritable.getPurchaseTime();
        return (time.hashCode() & Integer.MAX_VALUE) % numPartitions; // hash这种方式可能会把不同日期的数据放到同一个分区文件里面
    }

}
