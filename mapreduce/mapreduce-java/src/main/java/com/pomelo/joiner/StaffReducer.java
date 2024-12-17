package com.pomelo.joiner;

import com.pomelo.joiner.writable.StaffWritable;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * description: com.pomelo.joiner
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/12/17
 * version: 1.0
 */
public class StaffReducer extends Reducer<StaffWritable, StaffWritable, StaffWritable, NullWritable> {

    @Override
    protected void reduce(StaffWritable key, Iterable<StaffWritable> values
            , Reducer<StaffWritable, StaffWritable, StaffWritable, NullWritable>.Context context) throws IOException, InterruptedException {
        //  tip: 相同id的数据存储在values迭代器里面
        // why?  推测和比较器有关
        String address = "";
        List<StaffWritable> dataList = new ArrayList<>();
        for (StaffWritable value : values) {
            if (StringUtils.equalsIgnoreCase(value.getFlag(), "address")) {
                address = value.getAddress();
            }
            dataList.add(new StaffWritable(value.getId(), value.getName()
                    , value.getAge(), value.getAddress(), null)); // 必须新建，迭代器一直叠加会导致数据有变化
        }

        for (StaffWritable staffWritable : dataList) {
            staffWritable.setAddress(address);
            if (StringUtils.isAnyBlank(staffWritable.getAddress(), staffWritable.getName())) {
                continue;
            }
            context.write(staffWritable, NullWritable.get());
        }
    }
}
