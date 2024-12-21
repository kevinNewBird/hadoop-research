package com.pomelo.wordcount;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * description：flink 流式数据 wordcount
 *
 * @author zhaosong
 * @version 1.0
 * @company 北京海量数据有限公司
 * @date 2024/12/21 16:26
 */
public class StreamBatchWordCount {

    public static void main(String[] args) throws Exception {
        // 1.创建流式处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1.1.设置批数据处理模式
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);   // 指定批数据处理模式
//        env.setRuntimeMode(RuntimeExecutionMode.STREAMING); // 默认是流数据处理模式
//        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC); // 根据数据源的格式，自动决定采用什么处理模式。比如是文件，那么则自动采用批数据处理模式

        // 2.读取文件
        DataStreamSource<String> lines = env.readTextFile("./data/words.txt");

        // 3.切分单词并设置KV数据
        SingleOutputStreamOperator<Tuple2<String, Long>> kvWordsDS = lines.flatMap((FlatMapFunction<String, Tuple2<String, Long>>) (line, collector) -> {
            String[] words = StringUtils.split(line, " ");
            for (String word : words) {
                collector.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4.流式数据（即实时）分组统计获取word count结果 (根据tuple的0位置进行分组后，统计1位置)
        kvWordsDS.keyBy(tuple -> tuple.f0).sum(1).print();

        //5.流式计算中需要最后执行execute方法，否则不会打印结果 (和批数据处理的区别，流数据必须要触发执行)
        env.execute();
    }
}
