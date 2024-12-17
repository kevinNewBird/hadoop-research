package com.pomelo.udw.writable;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * description: car序列化
 * <tip/>
 * -由于是作为value, 不需要实现comparable接口。
 * -只有作为key时才需要去实现comparable接口，便于map-reduce的三次排序处理
 * <p>
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/12/17
 * version: 1.0
 */
public class CarWritable implements Writable {

    public CarWritable() {
        // 空构造实现
    }

    //属性
    @Getter
    @Setter
    private String carNo; // 车牌

    @Getter
    @Setter
    private Double avgSpeed; // 平均速度

    @Getter
    @Setter
    private Double totalMile; // 总里程数


    /**
     * 序列化数据
     *
     * @param out <code>DataOuput</code> to serialize this object into.
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        // 写出的顺序必须和读取的顺序保持一致
        out.writeUTF(carNo);
        out.writeDouble(avgSpeed);
        out.writeDouble(totalMile);
    }

    /**
     * 反序列化数据
     *
     * @param in <code>DataInput</code> to deseriablize this object from.
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        // 读取的顺序必须和写出的顺序保持一致
        this.carNo = in.readUTF();
        this.avgSpeed = in.readDouble();
        this.totalMile = in.readDouble();
    }

    @Override
    public String toString() {
        return "CarWritable{" +
                "carNo='" + carNo + '\'' +
                ", avgSpeed=" + avgSpeed +
                ", totalMile=" + totalMile +
                '}';
    }
}
