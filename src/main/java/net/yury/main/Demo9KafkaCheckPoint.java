package net.yury.main;

import net.yury.serialize.KafkaDeserializer;
import net.yury.sink.RichCountSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Demo9KafkaCheckPoint {
    private static String KAFKA_SERVER = "192.168.141.141:9092";
    private static String KAFKA_TOPIC = "test-topic";
    private static String KAFKA_GROUP_ID = "test_group_id";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 开启checkpoint
        env.setStateBackend(new FsStateBackend("hdfs://192.168.141.141:9000/flink-study2/ck1"));
        env.enableCheckpointing(5000);
        // 设置重启策略，出现异常重启3次，隔5秒重启一次
         env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)));
        // 系统异常退出或人为 Cancel 掉，不删除checkpoint数据
         env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 设置checkpoint模式
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);

        KafkaDeserializer kafkaDeserializer = new KafkaDeserializer();
        RichCountSink sink = new RichCountSink();
        env.setParallelism(1);
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_SERVER)
                .setTopics(KAFKA_TOPIC)
                .setGroupId(KAFKA_GROUP_ID)
//                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.committedOffsets())
//                .setProperty("commit.offsets.on.checkpoint", "true")
                .setDeserializer(kafkaDeserializer)
                .build();
        DataStreamSource<String> dataStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "kafka data source"
        );
        dataStream.addSink(sink).setParallelism(5);
        env.execute();
    }

}
