package com.pomelo.model.transformation;

import lombok.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

public class FlinkTransformationTest {

    /**
     * Transformation 类算子是 Apache Flink 中用于定义数据流处理的基本构建块。它们允许对DataStream数据流进行转换和操作，
     * 包括数据转换、数据操作和数据重组,通过Transformation类算子，可以对输入数据流进行映射、过滤、聚合等操作，
     * 生成新的DataStream数据流作为输出，以满足特定的处理需求。下面分别介绍Flink中常见的Transformation类算子。
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {
        // 1.综合测试
//        testAll();
        // 2.单个聚合方法验证
        testIterate();
    }

    private static void testAll() throws Exception {
        // 1. map -- 一对一
        testMap();

        // 2. flatMap: 扁平化操作 -- 一对多
        testFlatMap();

        // 3. filter： 过滤
        testFilter();

        // 4.keyBy: 基于键分组对数据进行聚合、合并或其他操作。
        testKeyBy();
        // 4.2.keyBy的相关操作：聚合、合并等
        testKeyByAggregations();
        // 4.3.reduce: 配合keyBy
        testKeyByReduce();

        // 5.union
        testUnion();

        // 6.connect
        testConnect();

        // 7.iterate
        testIterate();
    }


    /**
     * 功能：map用于对输入的DataStream数据流中的每个元素进行映射操作,
     * 它接受一个函数作为参数，该函数将每个输入元素转换为一个新的元素，并生成一个新的数据流作为输出。
     *
     * @throws Exception
     */
    private static void testMap() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> ds = env.fromCollection(Arrays.asList(1, 3, 5, 7, 9));
        // map
        // 以上DataStream经过Flink转换得到了“SingleOutputStreamOperator”类型，该类型继承了DataStream类。
        SingleOutputStreamOperator<Integer> mapTransformation = ds.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value + 1;
            }
        });

        mapTransformation.print();
        env.execute();
    }

    /**
     * 功能：flatMap算子用于对输入的DataStream中的每个元素进行扁平化映射操作的算子
     * 它接受一个函数作为参数，该函数将每个输入元素转换为零个或多个新的元素，并生成一个新的DataStream数据流作为输出
     * <p>
     * 与map算子不同，flatMap算子可以生成比输入更多的元素，因此可以用于扁平化操作
     */
    private static void testFlatMap() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> ds = env.fromCollection(Arrays.asList("1,2", "3,4", "5,6", "7,8"));
        SingleOutputStreamOperator<String> flatMapTransformation = ds.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] split = value.split(",");
                for (String s : split) {
                    out.collect(s);
                }
            }
        });

        flatMapTransformation.print();

        env.execute();
    }

    /**
     * 功能：filter算子用于对输入的DataStream中的元素进行条件过滤操作
     * 它接受一个函数作为参数，该函数针对每个输入元素返回一个布尔值，如果函数返回true，则输入元素将被保留在输出DataStream中，否则将被过滤掉。
     */
    private static void testFilter() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> ds = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8);
        SingleOutputStreamOperator<Integer> filterTransformation = ds.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {
                // 过滤奇数，只保留偶数
                return value % 2 == 0;
            }
        });

        filterTransformation.print();
        env.execute();
    }

    /**
     * 功能：KeyBy算子用于将输入的DataStream按照指定的键或键选择器函数进行分组操作
     * 它接受一个键选择器函数作为参数，该函数根据输入元素返回一个键，用于将数据流中的元素分组到不同的分区中，
     * 相同键的元素分配到同一个分区中，以便后续的操作可以基于键对数据进行聚合、合并或其他操作。
     * <p>
     * KeyBy算子使用时可以通过KeySelector函数来指定key键，DataStream通过KeyBy算子处理后得到的是KeyedStream对象
     * ，该对象也是DataStream。默认KeyBy算子会对数据流中指定的key键的hash值与Flink分区数（并行度）进行取模运算
     * ，从而决定该条数据后续被哪个并行度处理，
     * 如果Flink DataStream类型是POJOs类型，需要在该类型中重写hashCode方法，否则后续不能正确的将相同数据进行分组处理。
     */
    private static void testKeyBy() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<String, Integer>> ds = env.fromElements(
                Tuple2.of("a", 1),
                Tuple2.of("b", 2),
                Tuple2.of("c", 3),
                Tuple2.of("a", 4),
                Tuple2.of("b", 5)
        );

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = ds.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tp) throws Exception {
                // 指定key键,即第一个元素进行分组
                return tp.f0;
            }
        });

        // 以第一个元素进行分组，并以第二个元素进行统计。1表示第二个元素
        keyedStream.sum(1).print();
        env.execute();
    }

    /**
     * Aggregations（聚合函数）是Flink中用于对输入数据进行聚合操作的函数集合，它们可以应用于KeyedStream上，将一组输入元素聚合为一个输出元素。
     * <p/>
     * Flink提供了多种聚合函数，包括sum、min、minBy、max、maxBy,这些函数都是常见的聚合操作，作用如下：
     * <p>
     * - sum：针对输入keyedStream对指定列进行sum求和操作。
     * - min：针对输入keyedStream对指定列进行min最小值操作，结果流中其他列保持最开始第一条数据的值。
     * - minBy：同min类似，对指定的字段进行min最小值操作minBy返回的是最小值对应的整个对象。
     * - max：针对输入keyedStream对指定列进行max最大值操作，结果流中其他列保持最开始第一条数据的值。
     * - maxBy:同max类似，对指定的字段进行max最大值操作，maxBy返回的是最大值对应的整个对象。
     *
     * @throws Exception
     */
    private static void testKeyByAggregations() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //准备集合数据
        List<StationLog> list = Arrays.asList(
                new StationLog("sid1", "18600000000", "18600000001", "success", System.currentTimeMillis(), 120L),
                new StationLog("sid1", "18600000001", "18600000002", "fail", System.currentTimeMillis(), 30L),
                new StationLog("sid1", "18600000002", "18600000003", "busy", System.currentTimeMillis(), 50L),
                new StationLog("sid1", "18600000003", "18600000004", "barring", System.currentTimeMillis(), 90L),
                new StationLog("sid1", "18600000004", "18600000005", "success", System.currentTimeMillis(), 300L)
        );

        KeyedStream<StationLog, String> keyedStream = env.fromCollection(list)
                .keyBy(stationLog -> stationLog.sid);
        //统计duration 的总和
        keyedStream.sum("duration").print();
        //统计duration的最小值，min返回该列最小值，其他列与第一条数据保持一致
        keyedStream.min("duration").print();
        //统计duration的最小值，minBy返回的是最小值对应的整个对象
        keyedStream.minBy("duration").print();
        //统计duration的最大值，max返回该列最大值，其他列与第一条数据保持一致
        keyedStream.max("duration").print();
        //统计duration的最大值，maxBy返回的是最大值对应的整个对象
        keyedStream.maxBy("duration").print();

        env.execute();
    }

    /**
     * 功能：reduce算子是一种聚合算子，
     * 它接受一个函数作为参数，并将输入的KeyedStream中的元素进行两两聚合操作，
     * 该函数将两个相邻的元素作为输入参数，并生成一个新的DataStream数据流作为输出。
     * <p>
     * 与其他聚合函数如sum、min、max等不同，reduce算子的聚合函数可以自定义实现，因此可以适用于更广泛的聚合操作。
     * <p>
     * Reduce作用于KeyedStream，输出DataStream对象。
     */
    private static void testKeyByReduce() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, Integer>> ds = env.fromCollection(Arrays.asList(
                Tuple2.of("a", 1),
                Tuple2.of("b", 2),
                Tuple2.of("c", 3),
                Tuple2.of("a", 4),
                Tuple2.of("b", 5)));

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = ds.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tp) throws Exception {
                return tp.f0;
            }
        });

        keyedStream.reduce((v1, v2) -> Tuple2.of(v1.f0, v1.f1 + v2.f1)).print();
        env.execute();
    }

    /**
     * 功能：union算子是Flink流处理框架中数据流合并算子，
     * 可以将多个输入的DataStream多个数据流进行合并，并输出一个新的DataStream数据流作为结果，适用于需要将多个数据流合并为一个流的场景。
     * <p>
     * **需要注意的是union合并的数据流类型必须相同**，合并之后的数据流包含两个或多个流中所有元素，并且数据类型不变。
     */
    private static void testUnion() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> ds1 = env.fromCollection(Arrays.asList(1, 2, 3, 4));
        DataStreamSource<Integer> ds2 = env.fromCollection(Arrays.asList(5, 6, 7, 8));
        ds1.union(ds2).print();
        env.execute();
    }

    /**
     * connect算子将两个输入的DataStream数据流作为参数，将两个不同数据类型的DataStream数据流连接在一起，生成一个ConnectedStreams对象作为结果
     * <p>
     * 与union算子不同，union只是简单的将两个类型一样的流合并在一起，而connect算子可以将不同类型的DataStream连接在一起，并且connect只能连接两个流
     * <p>
     * connect生成的结果保留了两个输入流的类型信息，例如：dataStream1数据集为(String, Int)元祖类型，dataStream2数据集为Int类型，
     * 通过connect连接算子将两个不同数据类型的流结合在一起，其内部数据为\[(String, Int), Int\]的混合数据类型，保留了两个原始数据集的数据类型
     * <p>
     * 对于连接后的数据流可以使用map、flatMap、process等算子进行操作，
     * 但内部方法使用的是CoMapFunction、CoFlatMapFunction、CoProcessFunction等函数来进行处理，这些函数称作“**协处理函数**",
     * 分别接收两个输入流中的元素，并生成一个新的数据流作为输出，输出结果DataStream类型保持一致。
     */
    private static void testConnect() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<String, Integer>> ds1 = env.fromCollection(Arrays.asList(Tuple2.of("a", 1), Tuple2.of("b", 2), Tuple2.of("c", 3)));
        DataStreamSource<String> ds2 = env.fromCollection(Arrays.asList("aa", "bb", "cc"));
        ConnectedStreams<Tuple2<String, Integer>, String> connectStream = ds1.connect(ds2);
        SingleOutputStreamOperator<Tuple2<String, Integer>> processStream = connectStream.process(new CoProcessFunction<Tuple2<String, Integer>, String, Tuple2<String, Integer>>() {
            @Override
            public void processElement1(Tuple2<String, Integer> tp1, CoProcessFunction<Tuple2<String, Integer>, String, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> collector) throws Exception {
                collector.collect(tp1);
            }

            @Override
            public void processElement2(String value, CoProcessFunction<Tuple2<String, Integer>, String, Tuple2<String, Integer>>.Context context, Collector<Tuple2<String, Integer>> collector) throws Exception {
                collector.collect(Tuple2.of(value, 1));
            }
        });

        processStream.print();
        env.execute();
    }

    /**
     * iterate算子用于实现迭代计算的算子，它允许对输入的DataStream进行多次迭代操作，直到迭代条件不满足时迭代停止，该算子适合迭代计算场景.
     * 例如：机器学习中往往会对损失函数进行判断是否到达某个精度来判断训练是否需要结束就可以使用该算子来完成。
     * <p>
     * 该算子接受一个初始输入流作为起点，**整个迭代过程由两部分组成：迭代体和迭代条件**
     * 迭代体负责对输入的数据流进行处理，迭代条件用来判断本次流是否应该继续作为输入流反馈给迭代体继续迭代,
     * 满足条件的数据会继续作为输入流进入迭代体进行迭代计算直到不满足迭代条件为止
     * **注意:无论数据流是否满足迭代条件都会输出到下游**。
     */
    private static void testIterate() throws Exception {
        /**
         * 这里以Flink读取Socket数字为例，迭代体对输入数据进行减少操作，
         * 迭代条件判断数据是否小于0，数据不小于0时继续执行迭代体进行迭代计算，否则迭代计算终止。
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        DataStreamSource<String> ds1 = env.socketTextStream("node3", 9999);
        DataStreamSource<String> ds1 = env.fromElements("1", "2", "3", "4", "5", "6");
        //对数据流进行转换
        SingleOutputStreamOperator<Integer> ds2 = ds1.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                return Integer.valueOf(s);
            }
        });

        //使用 iterate() 方法创建一个迭代流 iterate，用于支持迭代计算
        IterativeStream<Integer> iterate = ds2.iterate();

        //定义迭代体：在迭代流 iterate 上进行映射转换，将每个整数元素减去 1，并返回一个新的数据流
        SingleOutputStreamOperator<Integer> minusOne = iterate.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                System.out.println("迭代体中输入的数据为：" + value);
                return value - 1;
            }
        });

        //定义迭代条件，满足迭代条件的继续进入迭代体进行迭代，否则不迭代
        SingleOutputStreamOperator<Integer> stillGreaterThanZero = minusOne.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {
                return value > 0;
            }
        });

        //对迭代流应用迭代条件
        iterate.closeWith(stillGreaterThanZero);

        //迭代流数据输出，无论是否满足迭代条件都会输出
        iterate.print();
        env.execute();
    }

    /**
     * StationLog基站日之类
     */
    @Getter
    @Setter
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    public static class StationLog {
        public String sid;
        public String callOut;
        public String callIn;
        public String callType;
        public Long callTime;
        public Long duration;
    }
}
