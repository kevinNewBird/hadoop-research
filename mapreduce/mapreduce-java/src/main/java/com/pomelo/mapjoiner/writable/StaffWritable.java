package com.pomelo.mapjoiner.writable;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * description: person.txt和address.txt数据整合结果
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/12/17
 * version: 1.0
 */
public class StaffWritable implements WritableComparable<StaffWritable> {

    public StaffWritable() {
        // 空构造
    }

    public StaffWritable(String id, String name, Integer age, String address, String flag) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.address = address;
        this.flag = flag;
    }

    // 1,张三,18,北京,flag
    // 属性
    @Getter
    @Setter
    private String id;

    @Getter
    @Setter
    private String name;

    @Getter
    @Setter
    private Integer age;

    @Getter
    @Setter
    private String address;

    @Getter
    @Setter
    private String flag; // 标识符，用于区分数据来自于哪个文件

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.id);
        out.writeUTF(this.name);
        out.writeInt(this.age);
        out.writeUTF(this.address);
        out.writeUTF(this.flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.name = in.readUTF();
        this.age = in.readInt();
        this.address = in.readUTF();
        this.flag = in.readUTF();
    }

    @Override
    public int compareTo(StaffWritable other) {
        return this.id.compareTo(other.id);
    }

    @Override
    public String toString() {
        return "StaffWritable{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", age=" + age +
                ", address='" + address + '\'' +
                '}';
    }
}
