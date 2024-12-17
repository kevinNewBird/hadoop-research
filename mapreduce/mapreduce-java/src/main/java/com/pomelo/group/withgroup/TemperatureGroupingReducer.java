package com.pomelo.group.withgroup;

import com.pomelo.group.writable.TemperatureWritable;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * description: com.pomelo.group.nogroup
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/12/17
 * version: 1.0
 */
public class TemperatureGroupingReducer extends Reducer<TemperatureWritable, TemperatureWritable
        , TemperatureWritable, NullWritable> {


    /**
     * @param key:    这里的key对应的是每一行数据
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(TemperatureWritable key, Iterable<TemperatureWritable> values
            , Reducer<TemperatureWritable, TemperatureWritable, TemperatureWritable, NullWritable>.Context context) throws IOException, InterruptedException {
        int cnt = 0;
        String day = "";// 记录日期天
        // 数据已经根据年月分好组
        Iterator<TemperatureWritable> iterator = values.iterator();

        while (iterator.hasNext()) {
            TemperatureWritable current = iterator.next();
            String currentDay = current.getDay();
            // 只需取出前两条数据
            if (cnt > 1) {
                break;
            }
            if (!StringUtils.equalsIgnoreCase(currentDay, day)) {
                context.write(current, NullWritable.get());
                day = currentDay;
                cnt++;
            }
        }
    }
}
