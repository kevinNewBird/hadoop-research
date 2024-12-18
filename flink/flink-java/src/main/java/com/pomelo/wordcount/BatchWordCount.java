package com.pomelo.wordcount;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * description：flink 批数据 wordcount
 * <p>
 * <tip/>
 * 数据来源： 文件data/words.txt
 * <p/>
 *
 * @author zhaosong
 * @version 1.0
 * @company 北京海量数据有限公司
 * @date 2024/12/18 22:06
 */
public class BatchWordCount {

    public static void main(String[] args) throws Exception {
        // 1.准备flink的本地运行环境（还有更多的其他的运行环境）
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2.读取文件数据 (一行一行的数据)
        DataSource<String> lineDs = env.readTextFile("./data/words.txt");

        // 3.对数据切分为单词
        // 3.1.(匿名内部类的方式)
//        FlatMapOperator<String, String> wordsDs = lineDs.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public void flatMap(String line, Collector<String> collector) throws Exception {
//                Arrays.stream(StringUtils.split(line, " ")).forEach(collector::collect);
//            }
//        });
        // 3.2.(函数式编程的方式)：flink存在类型擦除的情况，需要指定返回类型，否则会执行报错
        FlatMapOperator<String, String> wordsDs = lineDs.flatMap((String line, Collector<String> collector) ->
                Arrays.stream(StringUtils.split(line, " ")).forEach(collector::collect))
                // 指定返回类型
                .returns(Types.STRING);


        // 4.对单词进行计数
        // 4.1.(匿名内部类的方式)
//        MapOperator<String, Tuple2<String, Long>> countDs = wordsDs.map(new MapFunction<String, Tuple2<String, Long>>() {
//            @Override
//            public Tuple2<String, Long> map(String word) throws Exception {
//                return Tuple2.of(word, 1L);
//            }
//        });
        // 4.2.(函数式编程的方式)：flink存在类型擦除的情况，需要指定返回类型，否则会执行报错
        MapOperator<String, Tuple2<String, Long>> countDs = wordsDs.map( word -> Tuple2.of(word, 1L))
                // 指定返回类型
                .returns(Types.TUPLE(Types.STRING,Types.LONG));


        // 5.对单词分组统计 (根据Tuple2的0位置进行分组，1位置进行统计)
        /**
         * (hello,15)
         * (Spark,1)
         * (Scala,2)
         * (Java,2)
         * (MapReduce,1)
         * (Flink,9)
         */
        countDs.groupBy(0).sum(1).print();
    }
}
