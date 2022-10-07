package net.yury.main;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import net.yury.processfunction.MonitorProcessFunction;
import net.yury.serialize.DBDeserialization;
import net.yury.sink.PrintSink;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class Demo12monitor {
    public final static Properties properties = new Properties();
    public static Map<String, Map<String, List<Rule>>> config = new HashMap<>();
//    static {
//        InputStream is = Demo12monitor.class.getClassLoader().getResourceAsStream("flinkconfig.properties");
//        assert is != null;
//        try {
//            properties.load(is);
//        } catch (IOException e) {
//            e.printStackTrace();
//            System.exit(-1);
//        }
//        Map<String, List<Rule>> t1 = new HashMap<>();
//        t1.put("counts", List.of(
//                new Rule("count", "g", 60D)
//        ));
//        Map<String, List<Rule>> t2 = new HashMap<>();
//        t2.put("math", List.of(
//                new Rule("count", "l", 60D),
//                new Rule("count", "l", 40D)
//        ));
//        t2.put("chinese", List.of(
//                new Rule("chinese", "l", 65D),
//                new Rule("chinese", "l", 45D)
//        ));
//        t2.put("total", List.of(
//                new Rule("total", "l", 200D),
//                new Rule("total", "l", 150D)
//        ));
//        config.put("t1", t1);
//        config.put("t2", t2);
//    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        PostgreSQLSource.Builder<ObjectNode> builder = PostgreSQLSource.<ObjectNode>builder()
                .hostname(properties.getProperty("flink.cdc.monitor.hostname"))
                .port(Integer.parseInt(properties.getProperty("flink.cdc.monitor.host")))
                .username(properties.getProperty("flink.cdc.monitor.username"))
                .database(properties.getProperty("flink.cdc.monitor.database"))
                .deserializer(DBDeserialization.instance)
                .decodingPluginName(properties.getProperty("flink.cdc.monitor.decodingPluginName"));
        // password
        String password = properties.getProperty("flink.cdc.monitor.password");
        password = StringUtils.isEmpty(password)? "": password;
        builder.password(password);

        // schemaList
        String schemaList = properties.getProperty("flink.cdc.monitor.schemaList");
        if (!StringUtils.isEmpty(schemaList)) builder.schemaList(schemaList.split(","));

        // tableList
        String tableList = properties.getProperty("flink.cdc.monitor.tableList");
        if (!tableList.isEmpty()) builder.tableList(tableList.split(","));

        SourceFunction<ObjectNode> sourceFunction = builder.build();

        env.addSource(sourceFunction)
                .process(MonitorProcessFunction.function)
                .addSink(new PrintSink<ObjectNode>());
        env.execute("flink cdc monitor");
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Rule {
        public static final String GREATER_THAN = "g";
        public static final String LOWER_THAN = "l";

        private String field;
        private String judgeSign;
        private Double benchmark;

        public boolean decide(Double v) {
            if (GREATER_THAN.equals(judgeSign)) {
                return v > benchmark;
            }else if (LOWER_THAN.equals(judgeSign)) {
                return v < benchmark;
            }
            return false;
        }
    }
}