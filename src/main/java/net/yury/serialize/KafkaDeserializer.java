package net.yury.serialize;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;

public class KafkaDeserializer implements KafkaRecordDeserializationSchema<String> {
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<String> collector) {
        long offset = record.offset();
        String value = new String(record.value(), StandardCharsets.UTF_8);
        collector.collect("offset: " + offset + "; value: " + value);
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}
