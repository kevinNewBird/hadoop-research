package com.pomelo.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class FlinkKafkaSourceReadTest {

    /**
     * 用途：用于读取kafka的数据
     * 前置准备：
     * 1. 创建主题： ./kafka-topics.sh --bootstrap-server node1:9092 --create --topic flink-source-topic
     * 2. 生产消息： ./kafka-console-producer.sh --bootstrap-server node1:9092 --topic flink-source-topic
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        /**
         * 常用的TypeInformation有BasicTypeInfo、TupleTypeInfo、CaseClassTypeInfo、PojoTypeInfo类等，针对这些常用TypeInfomation介绍如下：
         *
         * * Flink通过实现BasicTypeInfo数据类型，能够支持任意Java原生基本（或装箱）类型和String类型，
         *   例如：Integer,String,Double等，除了BasicTypeInfo外，类似的还有BasicArrayTypeInfo，支持Java中数组和集合类型；
         * - 1.通过定义TupleTypeInfo来支持Tuple类型的数据；
         * - 2.通过CaseClassTypeInfo支持Scala Case Class ；
         * - 3.PojoTypeInfo可以识别任意的POJOs类,包括Java和Scala类，POJOs可以完成复杂数据架构的定义，但是在Flink中使用POJOs数据类型需要满足以下要求:
         *   - 3.1、POJOs类必须是Public修饰且独立定义，不能是内部类；
         *   - 3.2、POJOs 类中必须含有默认空构造器；
         *   - 3.3、POJOs类中所有的Fields必须是Public或者具有Public修饰的getter和Setter方法；
         *
         * 在使用Java API开发Flink应用时，通常情况下Flink都能正常进行数据类型推断进而选择合适的serializers以及comparators，
         * 但是在定义函数时如果使用到了泛型，JVM就会出现类型擦除的问题，Flink就获取不到对应的类型信息，这就需要借助类型提示（Type Hints）来告诉系统函数中传入的参数类型信息和输出类型，进而对数据类型进行推断处理
         */
        KafkaSource<Tuple2<String, String>> kafkaSource = KafkaSource.<Tuple2<String, String>>builder().setBootstrapServers("node1:9092,node2:9092,node3:9092").setTopics("flink-source-topic").setGroupId("flink-source-group").setStartingOffsets(OffsetsInitializer.latest())
//                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setDeserializer(new KafkaRecordDeserializationSchema<Tuple2<String, String>>() {
                    // 设置key，value数据获取后如何处理
                    @Override
                    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<Tuple2<String, String>> collector) throws IOException {
                        String key = null;
                        String value = null;
                        if (consumerRecord.key() != null) {
                            key = new String(consumerRecord.key(), "UTF-8");
                        }
                        if (consumerRecord.value() != null) {
                            value = new String(consumerRecord.value(), "UTF-8");
                        }
                        collector.collect(new Tuple2<>(key, value));
                    }

                    // 设置返回的二元组类型
                    @Override
                    public TypeInformation<Tuple2<String, String>> getProducedType() {
                        // 借助类型提示（Type Hints）来告诉系统函数中传入的参数类型信息和输出类型
                        return TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
                        });
                    }
                }).build();

        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source").print();

        env.execute();
    }
}
