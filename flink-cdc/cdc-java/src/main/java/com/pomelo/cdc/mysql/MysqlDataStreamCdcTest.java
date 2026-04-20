package com.pomelo.cdc.mysql;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 基于 DataStream API采集变更的mysql数据
 */
public class MysqlDataStreamCdcTest {

    // 注意： macos必须部署到虚拟机上，否则无法采集到变更的数据！！！
    public static void main(String[] args) throws Exception {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("172.16.16.151")      //设置MySQL hostname
                .port(3306)             //设置MySQL port
                .databaseList("testdb")    //设置捕获的数据库
                .serverTimeZone("Asia/Shanghai")
                .tableList("testdb.tbl1,testdb.tbl2") //设置捕获的数据表
                .username("test")       //设置登录MySQL用户名
                .password("Vbase@1234")     //设置登录MySQL密码
                .deserializer(new JsonDebeziumDeserializationSchema()) //设置序列化将SourceRecord 转换成 Json 字符串
                .startupOptions(StartupOptions.initial())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //开启checkpoint,设置3s的间隔
        env.enableCheckpointing(5000);

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(),"MySQL Source")
                .setParallelism(4) // 设置source节点的并行度为4
                .print().setParallelism(1); // 设置sink节点并行度为1

        env.execute("MySQL Source");
    }
}
