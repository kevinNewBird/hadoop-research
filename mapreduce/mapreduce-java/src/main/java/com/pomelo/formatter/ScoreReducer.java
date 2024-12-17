package com.pomelo.formatter;

import com.pomelo.formatter.writable.ScoreWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * description: com.pomelo.formatter
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/12/17
 * version: 1.0
 */
public class ScoreReducer extends Reducer<ScoreWritable, Text, ScoreWritable, NullWritable> {

    @Override
    protected void reduce(ScoreWritable key, Iterable<Text> values
            , Reducer<ScoreWritable, Text, ScoreWritable, NullWritable>.Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(key, NullWritable.get());
        }
    }
}
