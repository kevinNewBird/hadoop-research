package com.pomelo.group.withgroup.grouping;

import com.pomelo.group.writable.TemperatureWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * description: com.pomelo.group.withgroup.grouping
 * 自定义Reduced端分组比较器
 * 1) 自定义类要继承WritableComparator
 * 2) 自定义类中要实现空构造，在构造中调用父构造来创建实例
 * 3) 自定义类中实现compare方法
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/12/17
 * version: 1.0
 */
public class TemperatureGroupingComparator extends WritableComparator {


    public TemperatureGroupingComparator() {
        // 指定排序的类型是谁，并且是否要创建实例
        super(TemperatureWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        // 相同年月数据放在一起
        TemperatureWritable a1 = (TemperatureWritable) a;
        TemperatureWritable b1 = (TemperatureWritable) b;

        int yearCmp = a1.getYear().compareTo(b1.getYear());
        if (yearCmp == 0) {
            return a1.getMonth().compareTo(b1.getMonth());
        }
        return yearCmp;
    }
}
