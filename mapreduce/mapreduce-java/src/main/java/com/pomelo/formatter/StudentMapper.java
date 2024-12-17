package com.pomelo.formatter;

import com.pomelo.formatter.writable.StudentWritable;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * description: com.pomelo.formatter
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/12/17
 * version: 1.0
 */
public class StudentMapper extends Mapper<LongWritable, Text, StudentWritable, Text> {

    StudentWritable outputKey = new StudentWritable();

    @Override
    protected void map(LongWritable key, Text value
            , Mapper<LongWritable, Text, StudentWritable, Text>.Context context) throws IOException, InterruptedException {
        // 行数据
        String line = value.toString();
        String[] words = StringUtils.split(line, ",");

        outputKey.setName(words[0]);
        outputKey.setScore(Integer.valueOf(words[1]));

        // 写出数据
        context.write(outputKey, value);
    }
}
