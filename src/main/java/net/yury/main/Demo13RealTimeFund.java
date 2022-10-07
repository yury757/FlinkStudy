package net.yury.main;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import net.yury.processfunction.FundProcessFunction;
import net.yury.serialize.DBDeserialization;
import net.yury.sink.PrintSink;
import net.yury.utils.DruidUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class Demo13RealTimeFund {
    static {
        Class<DruidUtil> druidUtilClass = DruidUtil.class; // 加载类
    }
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SourceFunction<ObjectNode> pgSource = PostgreSQLSource.<ObjectNode>builder()
                .hostname("192.168.141.141")
                .port(5432)
                .database("postgres")
                .schemaList("public")
                .tableList("public.fund_quote")
                .username("postgres")
                .password("postgres")
                .deserializer(new DBDeserialization())
                .decodingPluginName("pgoutput")
                .build();

        env.addSource(pgSource)
                .process(new FundProcessFunction())
                .addSink(new PrintSink<ObjectNode>())
                .setParallelism(1);
        env.execute("Demo13RealTimeFund");
        System.out.println("Demo13RealTimeFund started");
    }
}
