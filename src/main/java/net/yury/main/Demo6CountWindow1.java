package net.yury.main;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Demo6CountWindow1 {
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

        inputDataStream
                .flatMap(new Demo1WordCount.MyFlatMapper())
                .keyBy(tuple -> tuple.f0)
                .window(tumblingTimeWindows) // 滚动时间窗口
                ;
        env.execute(jobName);
    }
}
