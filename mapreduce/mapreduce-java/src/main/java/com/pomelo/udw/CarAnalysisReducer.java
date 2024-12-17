package com.pomelo.udw;

import com.pomelo.udw.writable.CarWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * description: com.pomelo.uds
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/12/17
 * version: 1.0
 */
public class CarAnalysisReducer extends Reducer<Text, CarWritable, Text, CarWritable> {
    // （不输出key）这里可以有个小改造，即把输出Text更换为NullWritable, 表示不输出key
//public class CarAnalysisReducer extends Reducer<Text, CarWritable, NullWritable, CarWritable> {

    // 定义输出Value
    CarWritable outputValue = new CarWritable();

    @Override
    protected void reduce(Text key, Iterable<CarWritable> values
            , Reducer<Text, CarWritable, Text, CarWritable>.Context context) throws IOException, InterruptedException {
        // 1.统计当前key的values
        int cnt = 0;
        double totalSpeed = 0; // 总速度
        double totalMile = 0; // 总里程数
        for (CarWritable value : values) {
            outputValue.setCarNo(value.getCarNo());
            totalSpeed += value.getAvgSpeed();
            totalMile += value.getTotalMile();
            cnt++;
        }

        DecimalFormat df = new DecimalFormat("#.00"); // 指定保留多少位小数
        outputValue.setAvgSpeed(Double.valueOf(df.format(totalSpeed / cnt)));
        outputValue.setTotalMile(totalMile);

        // 写出数据
        context.write(key, outputValue);
        // （不输出key）写出数据
//        context.write(NullWritable.get(), outputValue);
    }
}
