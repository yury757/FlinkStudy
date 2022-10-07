package net.yury.main;

import net.yury.sink.RichCountSink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import java.util.Properties;

public class Demo8KafkaSourceOld {
    private static String KAFKA_SERVER = "192.168.141.141:9092";
    private static String KAFKA_TOPIC = "test-topic";
    private static String KAFKA_GROUP_ID = "test_group_id";
    private static String KAFKA_AUTO_COMMIT = "true";
    private static String KAFKA_COMMIT_TIME = "5000";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        RichCountSink sink = new RichCountSink();
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, KAFKA_GROUP_ID);
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, KAFKA_AUTO_COMMIT);
        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, KAFKA_COMMIT_TIME);

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                KAFKA_TOPIC,
                new SimpleStringSchema(),
                properties);
        consumer.setStartFromEarliest();
        DataStream<String> stream = env.addSource(consumer);
        stream.addSink(sink);
        env.execute();
    }
}
