package com.pomelo.model.source;

import lombok.*;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FlinkCollectionSourceTest {

    /**
     * 用途：用于程序测试
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<StationLog> stationLogs = new ArrayList<>(Arrays.asList(
                StationLog.builder().sid("001").callOut("186").callIn("187").callType("busy").callTime(1000L).duration(0L).build(),
                StationLog.builder().sid("002").callOut("187").callIn("186").callType("fail").callTime(2000L).duration(0L).build(),
                StationLog.builder().sid("003").callOut("186").callIn("188").callType("busy").callTime(3000L).duration(0L).build(),
                StationLog.builder().sid("004").callOut("188").callIn("186").callType("busy").callTime(4000L).duration(0L).build(),
                StationLog.builder().sid("005").callOut("188").callIn("187").callType("busy").callTime(5000L).duration(0L).build()
                ));
        // 或者env.fromElements(...)
        DataStreamSource<StationLog> ds = env.fromCollection(stationLogs);
        ds.print();

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
    public static class StationLog{
        public String sid;
        public String callOut;
        public String callIn;
        public String callType;
        public Long callTime;
        public Long duration;
    }
}
