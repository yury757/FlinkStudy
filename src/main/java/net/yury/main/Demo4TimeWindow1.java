package net.yury.main;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Demo4TimeWindow1 {

    public static void main(String[] args) throws Exception {
        String jobName = args[0];
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4); // 设置并行的操作的线程数

        String host = args[1];
        int port = Integer.parseInt(args[2]);
        // 在linux中使用nc -lk 7777命令，nc即netcat，是一个基于网络连接发送纯文本数据的功能。
        DataStream<String> inputDataStream = env.socketTextStream(host, port);

        // 滚动时间窗口
        TumblingProcessingTimeWindows tumblingTimeWindows = TumblingProcessingTimeWindows.of(Time.seconds(5));
        // 滑动时间窗口
        SlidingProcessingTimeWindows slidingTimeWindows = SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(1));
        // session窗口
        EventTimeSessionWindows sessionWindows = EventTimeSessionWindows.withGap(Time.seconds(5));

        AggregateFunction<Tuple2<String, Integer>, Integer, Integer> aggregateFunction = new AggregateFunction<Tuple2<String, Integer>, Integer, Integer>() {
            // 创建初始状态
            @Override
            public Integer createAccumulator() {
                return 0;
            }

            // 新数据处理逻辑
            @Override
            public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
                return accumulator + value.f1;
            }

            // 获取结果
            @Override
            public Integer getResult(Integer accumulator) {
                return accumulator;
            }

            // 对两个历史状态进行合并的逻辑
            @Override
            public Integer merge(Integer a, Integer b) {
                return a + b;
            }
        };

        SingleOutputStreamOperator<Integer> aggregate = inputDataStream
                .flatMap(new Demo1WordCount.MyFlatMapper())
                .keyBy(tuple -> tuple.f0)
                .window(tumblingTimeWindows) // 滚动时间窗口
//                .window(slidingTimeWindows) // 滑动时间窗口
//                .countWindow(10) // 滚动计数窗口
//                .countWindow(10, 2) // 滚动滑动窗口
//                .window(sessionWindows) // session窗口
//                .timeWindow(Time.seconds(5)) // v1.12.5 时间滚动窗口，该方法已经被废弃
                .aggregate(aggregateFunction);
        aggregate.print();
        env.execute(jobName);
    }

}
