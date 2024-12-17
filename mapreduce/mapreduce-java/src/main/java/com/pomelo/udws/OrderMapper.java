package com.pomelo.udws;

import com.pomelo.udws.writable.OrderWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * description: com.pomelo.udws
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/12/17
 * version: 1.0
 */
public class OrderMapper extends Mapper<LongWritable, Text, OrderWritable, OrderWritable> {

    OrderWritable outputKey = new OrderWritable();

    @Override
    protected void map(LongWritable key, Text value
            , Mapper<LongWritable, Text, OrderWritable, OrderWritable>.Context context) throws IOException, InterruptedException {
        // 行数据：1001	2024-03-10	商品A	2	100
        String line = value.toString();

        String[] words = line.split("\t");
        outputKey.setOrderNo(words[0]);
        outputKey.setPurchaseTime(words[1]);
        outputKey.setGoodsName(words[2]);
        outputKey.setPurchaseNum(Integer.parseInt(words[3]));
        outputKey.setAmount(Double.valueOf(words[4]));

        // 写出数据
        context.write(outputKey, outputKey);
    }
}
