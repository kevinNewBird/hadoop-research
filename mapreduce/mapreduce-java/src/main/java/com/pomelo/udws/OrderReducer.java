package com.pomelo.udws;

import com.pomelo.udws.writable.OrderWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * description: com.pomelo.udws
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/12/17
 * version: 1.0
 */
public class OrderReducer extends Reducer<OrderWritable, NullWritable, OrderWritable, NullWritable> {


    @Override
    protected void reduce(OrderWritable key, Iterable<NullWritable> values
            , Reducer<OrderWritable, NullWritable, OrderWritable, NullWritable>.Context context) throws IOException, InterruptedException {
        // 必须循环写出数据，可能存在相同的商品购买金额数据，从而被覆盖
        for (NullWritable value : values) {
            // 写出数据（因为商品没有重复，暂不涉及分组）
            context.write(key, NullWritable.get());
        }
    }
}
