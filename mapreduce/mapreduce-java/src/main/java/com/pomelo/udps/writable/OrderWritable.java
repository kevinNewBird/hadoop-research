package com.pomelo.udps.writable;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * description: com.pomelo.udws.writable
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/12/17
 * version: 1.0
 */
public class OrderWritable implements Writable, WritableComparable<OrderWritable> {

    // 空实现构造
    public OrderWritable() {
    }

    // 属性
    @Getter
    @Setter
    private String orderNo; // 订单号

    @Getter
    @Setter
    private String purchaseTime; // 购买时间

    @Getter
    @Setter
    private String goodsName; // 商品名称

    @Getter
    @Setter
    private int purchaseNum; // 购买数量

    @Getter
    @Setter
    private Double amount; // 购买金额

    /**
     * 序列化
     *
     * @param out <code>DataOuput</code> to serialize this object into.
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.orderNo);
        out.writeUTF(this.purchaseTime);
        out.writeUTF(this.goodsName);
        out.writeInt(this.purchaseNum);
        out.writeDouble(this.amount);
    }

    /**
     * 反序列化
     *
     * @param in <code>DataInput</code> to deseriablize this object from.
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderNo = in.readUTF();
        this.purchaseTime = in.readUTF();
        this.goodsName = in.readUTF();
        this.purchaseNum = in.readInt();
        this.amount = in.readDouble();
    }

    /**
     * 比较器
     *
     * @param other the object to be compared.
     * @return
     */
    @Override
    public int compareTo(OrderWritable other) {
        // 根据金额进行排序(降序排序)
        if (this.amount > other.amount) {
            return -1;
        } else if (this.amount < other.amount) {
            return 1;
        } else {
            // 金额相等时，根据购买数量降序排序
            return other.purchaseNum - this.purchaseNum;
        }
    }

    @Override
    public String toString() {
        return "OrderWritable{" +
                "orderNo='" + orderNo + '\'' +
                ", purchaseTime='" + purchaseTime + '\'' +
                ", goodsName='" + goodsName + '\'' +
                ", purchaseNum=" + purchaseNum +
                ", amount=" + amount +
                '}';
    }
}
