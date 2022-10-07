package net.yury.main;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * 流处理word count
 * 同样，先创建执行环境，再读取数据，再基于数据流进行数据处理，最后打印输出
 */
public class Demo2WordCount {
    public static void main(String[] args) throws Exception {
        String jobName = args[0];
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2); // 设置并行的操作的线程数

        String path = args[1];
        System.out.println("2 " + Arrays.toString(args));
        DataStreamSource<String> stringDataSource = env.readTextFile(path);

        SingleOutputStreamOperator<Tuple2<String, Integer>> res = stringDataSource.flatMap(new Demo1WordCount.MyFlatMapper())
                .keyBy(tuple -> tuple.f0)
                .sum(1);
        res.print();
        env.execute(jobName);
    }
}
