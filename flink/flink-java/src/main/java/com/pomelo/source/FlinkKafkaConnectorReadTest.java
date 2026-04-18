package com.pomelo.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkKafkaConnectorReadTest {

    /**
     * 前置准备：
     * 1. 创建主题： ./kafka-topics.sh --bootstrap-server node1:9092 --create --topic flink-source-topic
     * 2. 生产消息： ./kafka-console-producer.sh --bootstrap-server node1:9092 --topic flink-source-topic
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder().setBootstrapServers("node1:9092,node2:9092,node3:9092")
                .setTopics("flink-source-topic")
                .setGroupId("flink-source-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        env.fromSource(kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "kafka-source").print();

        env.execute();
    }
}
