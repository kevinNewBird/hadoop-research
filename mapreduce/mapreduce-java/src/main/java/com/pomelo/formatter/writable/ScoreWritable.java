package com.pomelo.formatter.writable;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * description: com.pomelo.formatter.format
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/12/17
 * version: 1.0
 */
public class ScoreWritable implements Writable, WritableComparable<ScoreWritable> {

    public ScoreWritable() {
        // 空构造
    }

    // 属性
    @Getter
    @Setter
    private String name; // 姓名

    @Getter
    @Setter
    private Integer score; // 分数

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.name);
        out.writeInt(this.score);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.name = in.readUTF();
        this.score = in.readInt();
    }

    @Override
    public int compareTo(ScoreWritable other) {
        return this.score.compareTo(other.score);
    }

    @Override
    public String toString() {
        return "StudentWritable{" +
                "name='" + name + '\'' +
                ", score=" + score +
                '}';
    }
}
