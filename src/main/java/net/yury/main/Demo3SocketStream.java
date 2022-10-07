package net.yury.main;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 从socket中读取流数据，并对单词进行计数
 */
public class Demo3SocketStream {
    public static void main(String[] args) throws Exception {
        String jobName = args[0];
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2); // 设置并行的操作的线程数

        String host = args[1];
        int port = Integer.parseInt(args[2]);
        // 在linux中使用nc -lk 7777命令，nc即netcat，是一个基于网络连接发送纯文本数据的功能。
        DataStream<String> inputDataStream = env.socketTextStream(host, port);

        SingleOutputStreamOperator<Tuple2<String, Integer>> res = inputDataStream.flatMap(new Demo1WordCount.MyFlatMapper()) // 默认slot共享组名称为default
                .keyBy(tuple -> tuple.f0)
                .sum(1)
                .setParallelism(2)
                .slotSharingGroup("red"); // 设置slot共享组名称为red
        res.print().setParallelism(1).slotSharingGroup("green"); // 设置slot共享组名称为green
        env.execute(jobName);
    }
}
