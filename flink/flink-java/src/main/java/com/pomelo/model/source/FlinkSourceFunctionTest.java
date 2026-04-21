package com.pomelo.model.source;

import lombok.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * 对于一些其他的数据源，我们也可以实现自定义Source进行实时数据获取。自定义数据源有两种实现方式：
 * <p>
 * * 通过实现SourceFunction接口来自定义无并行度（也就是并行度只能为1）的Source。
 * * 通过实现ParallelSourceFunction 接口或者继承RichParallelSourceFunction 来自定义有并行度的数据源。
 * <p>
 * 无论是那种接口实现方式都需要重写以下两个方法：
 * <p>
 * 1. run()：大部分情况下都需要在run方法中实现一个循环产生数据，通过Flink上下文对象传递到下游。
 * 2. cancel():当取消对应的Flink任务时被调用。
 */
public class FlinkSourceFunctionTest {

    /**
     * Flink读取自定义Source，并行度为1
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new MyDefinedNoParallelSource()).print();
        env.execute("custom no-parallel source");
    }


    /**
     * 自定义非并行Source
     */
    private static class MyDefinedNoParallelSource implements SourceFunction<StationLog> {
        boolean flag = true;

        /**
         * 主要方法:启动一个Source，大部分情况下都需要在run方法中实现一个循环产生数据
         * 这里计划每秒产生1条基站数据
         *
         * @param ctx
         * @throws Exception
         */
        @Override
        public void run(SourceContext<StationLog> ctx) throws Exception {
            Random random = new Random();
            String[] callTypes = {"fail", "success", "busy", "barring"};
            while (flag) {
                String sid = "sid_" + random.nextInt(10);
                String callOut = "1811234" + (random.nextInt(9000) + 1000);
                String callIn = "1915678" + (random.nextInt(9000) + 1000);
                String callType = callTypes[random.nextInt(4)];
                Long callTime = System.currentTimeMillis();
                Long durations = Long.valueOf(random.nextInt(50) + "");
                ctx.collect(new StationLog(sid, callOut, callIn, callType, callTime, durations));
                Thread.sleep(1000);//1s 产生一个事件
            }
        }

        /**
         * 当取消对应的Flink任务时被调用
         */
        @Override
        public void cancel() {
            flag = false;
        }
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
