package com.pomelo.joiner;

import com.pomelo.joiner.writable.StaffWritable;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * description: 读取的person.txt
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/12/17
 * version: 1.0
 */
public class AddressMapper extends Mapper<LongWritable, Text, StaffWritable, StaffWritable> {

    StaffWritable outputKey = new StaffWritable();


    @Override
    protected void map(LongWritable key, Text value
            , Mapper<LongWritable, Text, StaffWritable, StaffWritable>.Context context) throws IOException, InterruptedException {
        // 行数据: 1,北京
        String line = value.toString();
        String[] words = StringUtils.split(line, ",");

        outputKey.setId(words[0]);
        outputKey.setName("");
        outputKey.setAge(0);
        outputKey.setAddress(words[1]);
        outputKey.setFlag("address"); // 设置标识

        // 写出数据
        context.write(outputKey, outputKey);
    }
}