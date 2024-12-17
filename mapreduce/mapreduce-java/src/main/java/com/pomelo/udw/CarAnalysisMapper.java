package com.pomelo.udw;

import com.pomelo.udw.writable.CarWritable;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * description: com.pomelo.uds
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/12/17
 * version: 1.0
 */
public class CarAnalysisMapper extends Mapper<LongWritable, Text, Text, CarWritable> {

    // 定义输出key
    Text outputKey = new Text();

    // 定义输出value
    CarWritable outputValue = new CarWritable();


    @Override

    protected void map(LongWritable key, Text value
            , Mapper<LongWritable, Text, Text, CarWritable>.Context context) throws IOException, InterruptedException {
        // 1.读取到的行数据： 10,皖OBMCM,95,1000
        String line = value.toString();
        // 按照逗号分隔
        String[] words = StringUtils.split(line, ",");

        // 2.设置outputKey
        outputKey.set(words[1]);
        // 3.设置outputValue
        outputValue.setCarNo(words[1]);
        outputValue.setAvgSpeed(Double.valueOf(words[2]));
        outputValue.setTotalMile(Double.valueOf(words[3]));

        // 4.写出数据
        context.write(outputKey, outputValue);
    }
}
