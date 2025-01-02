package com.pomelo.wordcount;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * description：基于socket网络的实时流计算（nc -lk 9999）
 * </p>
 * 开启本地webui界面，而无需搭建集群
 *
 * @author zhaosong
 * @version 1.0
 * @company 北京海量数据有限公司
 * @date 2025/1/1 22:46
 */
public class SocketWordCountByLocalWebUI {

    public static void main(String[] args) throws Exception {
        // 0.配置本地webui端口
        Configuration conf = new Configuration();
        conf.set(RestOptions.BIND_PORT, "8081");
        // 1.准备运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        // 2.读取socket数据(客户端在node3节点上)
        DataStreamSource<String> ds = env.socketTextStream("node2", 9999);

        // 3.数据分组及统计
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordsDs = ds.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String word : StringUtils.split(line, ",")) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        });

        wordsDs.keyBy(tb -> tb.f0).sum(1).print();

        // 4.执行
        env.execute();


    }

}
