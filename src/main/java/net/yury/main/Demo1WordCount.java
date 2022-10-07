package net.yury.main;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * 批处理word count
 */
public class Demo1WordCount {
    public static void main(String[] args) throws Exception {
        String jobName = args[0];
        String path = args[1];

        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfiguration().setString("job.name", jobName);

        // 从文件中读取数据
        System.out.println("1 " + Arrays.toString(args));
        DataSet<String> stringDataSource = env.readTextFile(path);

        // 对数据集进行处理，按空格分词展开
        DataSet<Tuple2<String, Integer>> res = stringDataSource.flatMap(new MyFlatMapper())
                .groupBy(0) // 按照第0个位置的值group by
                .sum(1); // group后分组统计第1个位置的值的sum
        res.print();
    }

    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>>{
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
            // 按空格分词
            String[] words = value.split(" ");
            for (String word: words) {
                collector.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
