package net.yury.main;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import net.yury.serialize.DBDeserialization;
import net.yury.sink.PrintSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class Demo11PostgresCDC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SourceFunction<ObjectNode> pgSource = PostgreSQLSource.<ObjectNode>builder()
                .hostname("192.168.141.141")
                .port(5432)
                .database("postgres")
                .schemaList("public")
                .tableList("public.t1")
                .username("postgres")
                .password("postgres")
                .deserializer(new DBDeserialization())
                .decodingPluginName("pgoutput")
                .build();

        env.addSource(pgSource)
                .addSink(new PrintSink<ObjectNode>())
                .setParallelism(2);
        env.execute("Print postgresql Snapshot + Binlog");
    }
}
