package net.yury.main;

import net.yury.serialize.KafkaDeserializer;
import net.yury.sink.RichCountSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Demo7KafkaSource {
    private static String KAFKA_SERVER = "192.168.141.141:9092";
    private static String KAFKA_TOPIC = "test-topic";
    private static String KAFKA_GROUP_ID = "test_group_id";
    private static OffsetsInitializer KAFKA_OFFSET = OffsetsInitializer.earliest();

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaDeserializer kafkaDeserializer = new KafkaDeserializer();
        RichCountSink sink = new RichCountSink();
        env.setParallelism(1);
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_SERVER)
                .setTopics(KAFKA_TOPIC)
                .setGroupId(KAFKA_GROUP_ID)
                .setStartingOffsets(KAFKA_OFFSET)
                .setDeserializer(kafkaDeserializer)
                .build();
        DataStreamSource<String> dataStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "kafka data source"
        );
        dataStream.addSink(sink);
        env.execute();
    }
}
