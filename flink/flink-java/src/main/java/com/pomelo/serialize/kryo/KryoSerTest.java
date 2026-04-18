package com.pomelo.serialize.kryo;

import com.pomelo.serialize.base.Student;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class KryoSerTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
       // 注册自定义的Kryo的序列化器
        env.registerTypeWithKryoSerializer(Student.class, StudentSerializer.class);

        // 用户基本信息
        SingleOutputStreamOperator<Student> ds = env
                // 1.datasource
                .fromCollection(Arrays.asList(
                        "1,zs,18",
                        "2,ls,20",
                        "3,ww,19"))
                // 2.transformation -- 转换
                .map(new MapFunction<String, Student>() {
                    @Override
                    public Student map(String s) throws Exception {
                        String[] split = StringUtils.split(s, ',');
                        return Student.builder()
                                .id(Integer.parseInt(split[0]))
                                .name(split[1])
                                .age(Integer.parseInt(split[2]))
                                .build();
                    }
                }).returns(Types.GENERIC(Student.class));

        // 2.transformation -- 过滤
        SingleOutputStreamOperator<Student> fds = ds.filter(new FilterFunction<Student>() {
            @Override
            public boolean filter(Student student) throws Exception {
                return student.getId() > 0;
            }
        });

        // 3.data sink
        fds.print();

        // 4.执行
        env.execute();
    }
}
