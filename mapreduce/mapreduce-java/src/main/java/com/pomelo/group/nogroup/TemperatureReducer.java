package com.pomelo.group.nogroup;

import com.pomelo.group.writable.TemperatureWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.*;

/**
 * description: com.pomelo.group.nogroup
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/12/17
 * version: 1.0
 */
public class TemperatureReducer extends Reducer<TemperatureWritable, TemperatureWritable
        , TemperatureWritable, NullWritable> {

    int cnt = 0;

    List<TemperatureWritable> groupList = new ArrayList<>();

    Map<String, String> marked = new HashMap<>();

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
        // 遍历所有的数据， 放入一个集合中
        Iterator<TemperatureWritable> iterator = values.iterator();
        while (iterator.hasNext()) {
            TemperatureWritable current = iterator.next();
            groupList.add(current);
        }

        // 遍历并取出相同年月，不同日期的top2
        for (TemperatureWritable temp : groupList) {
            String year = temp.getYear();
            String month = temp.getMonth();
            String day = temp.getDay();

            String date = MessageFormat.format("{0}-{1}", year, month);
            // 第一次处理当前年月的数据，将这条数据写出
            if (!marked.containsKey(date)) {
                cnt = 1;
                context.write(temp, NullWritable.get());
                marked.put(date, day + "," + cnt);
                continue;
            }

            // 当前年月数据有多条，判断天是否相同，不相同就输出，相同跳过
            if (marked.containsKey(date) && !day.equals(marked.get(date).split(",")[0])) {
                cnt = Integer.parseInt(marked.get(date).split(",")[1]);
                cnt++;
                if (cnt == 2) {
                    context.write(temp, NullWritable.get());
                }
                marked.put(date, day + "," + cnt);
            }
        }
    }
}
