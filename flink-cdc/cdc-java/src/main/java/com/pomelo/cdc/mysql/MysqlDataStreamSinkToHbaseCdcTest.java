package com.pomelo.cdc.mysql;

import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class MysqlDataStreamSinkToHbaseCdcTest {

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
        env.enableCheckpointing(5000);

        // 转换数据为json类型
        SingleOutputStreamOperator<JSONObject> ds = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "Mysql source sink to hbase").map((new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                return JSONObject.parse(value);
            }
        }));

        // 将数据写出到hbase
        ds.addSink(new RichSinkFunction<JSONObject>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void invoke(JSONObject value, Context context) throws Exception {

            }
            @Override
            public void close() throws Exception {

            }
        });

        env.execute();
    }
}
