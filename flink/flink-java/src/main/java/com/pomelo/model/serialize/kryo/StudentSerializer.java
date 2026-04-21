package com.pomelo.model.serialize.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.pomelo.model.serialize.base.Student;

/**
 * Flink提供了多种序列化器，包括Kryo、Avro和Java序列化器等，大多数情况下，用户不用担心flink的序列化框架，
 * Flink会通过TypeInfomation在数据处理之前推断数据类型，进而使用对应的序列化器，例如：针对标准类型（int,
 * double,long,string）直接由Flink自带的序列化器处理，其他类型默认会交给Kryo处理。
 * <p>
 * 但是对于Kryo仍然无法处理的类型，可以采取以下两种解决方案：
 * -1.强制使用Avro替代Kryo序列化：env.getConfig().enableForceAvro()， 设置序列化方式为avro
 * -2.自定义注册Kryo序列化，即env.getConfig().registerTypeWithKryoSerializer(..)
 * <p>
 * 本类即为第二种方式实现的Kryo序列化器
 */
public class StudentSerializer extends Serializer<Student> {

    @Override
    public void write(Kryo kryo, Output output, Student o) {
        output.writeInt(o.getId());
        output.writeString(o.getName());
        output.writeInt(o.getAge());
    }

    @Override
    public Student read(Kryo kryo, Input input, Class aClass) {
        int id = input.readInt();
        String name = input.readString();
        int age = input.readInt();
        return Student.builder()
                .id(id)
                .name(name)
                .age(age)
                .build();
    }
}
