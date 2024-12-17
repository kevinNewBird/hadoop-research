package com.pomelo.combiner.combine;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * description: com.pomelo.combiner.combine
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/12/17
 * version: 1.0
 */
// 注意：输入和输出的数据泛型都应该和Mapper的保持一致，即WordCountMapper的输出K,V类型
public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    IntWritable total = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values
            , Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }

        // 设置当前key对应的value值，就是当前单词对应的 次数
        total.set(sum);
        context.write(key, total);
    }
}
