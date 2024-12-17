package com.pomelo.group.nogroup;

import com.pomelo.group.writable.TemperatureWritable;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * description: com.pomelo.group.nogroup
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/12/17
 * version: 1.0
 */
public class TemperatureMapper extends Mapper<LongWritable, Text, TemperatureWritable, TemperatureWritable> {

    TemperatureWritable outputKey = new TemperatureWritable();

    @Override
    protected void map(LongWritable key, Text value
            , Mapper<LongWritable, Text, TemperatureWritable, TemperatureWritable>.Context context) throws IOException, InterruptedException {
        // 1.行数据： 2024-08-23	23
        String line = value.toString();

        // 2.设置数据
        String[] words = StringUtils.split(line, "\t");
        String[] dates = StringUtils.split(words[0], "-");
        outputKey.setYear(dates[0]);
        outputKey.setMonth(dates[1]);
        outputKey.setDay(dates[2]);
        outputKey.setTemperature(Integer.valueOf(words[1]));

        // 写出数据
        context.write(outputKey, outputKey);
    }
}
