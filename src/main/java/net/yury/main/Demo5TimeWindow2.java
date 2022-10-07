package net.yury.main;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Demo5TimeWindow2 {

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

        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> apply = inputDataStream
                .flatMap(new Demo1WordCount.MyFlatMapper())
                .keyBy(tuple -> tuple.f0)
                .window(tumblingTimeWindows) // 滚动时间窗口
                .apply(new WindowFunction<Tuple2<String, Integer>, Tuple3<String, Long, Integer>, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                        int count = 0;
                        for (Tuple2<String, Integer> tuple : input) {
                            count++;
                        }
                        Tuple3<String, Long, Integer> res = new Tuple3<>();
                        res.f0 = s;
                        res.f1 = window.getEnd();
                        res.f2 = count;
                        out.collect(res);
                    }
                });
        apply.print();
        env.execute(jobName);
    }

}
