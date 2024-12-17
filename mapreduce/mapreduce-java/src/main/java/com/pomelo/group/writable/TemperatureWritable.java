package com.pomelo.group.writable;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * description: com.pomelo.group.writable
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/12/17
 * version: 1.0
 */
public class TemperatureWritable implements Writable, WritableComparable<TemperatureWritable> {

    public TemperatureWritable() {
        // 空构造
    }

    // 属性
    @Getter
    @Setter
    private String year;  //年

    @Getter
    @Setter
    private String month; // 月

    @Getter
    @Setter
    private String day; // 日

    @Getter
    @Setter
    private Integer temperature; // 温度

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.year);
        out.writeUTF(this.month);
        out.writeUTF(this.day);
        out.writeInt(this.temperature);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.year = in.readUTF();
        this.month = in.readUTF();
        this.day = in.readUTF();
        this.temperature = in.readInt();
    }

    @Override
    public int compareTo(TemperatureWritable other) {
        // 年月上的排序是无所谓的，只需要关注温度
        int yearCmp = this.year.compareTo(other.year); // 此时是升序
        int monthCmp = this.month.compareTo(other.month); // 此时是升序
        if (yearCmp == 0) {
            if (monthCmp == 0) {
                // 比较温度，降序排序
                return other.temperature - this.temperature;
            }
            return monthCmp;
        }
        return yearCmp;
    }

    @Override
    public String toString() {
        return "TemperatureWritable{" +
                "year='" + year + '\'' +
                ", month='" + month + '\'' +
                ", day='" + day + '\'' +
                ", temperature=" + temperature +
                '}';
    }
}
