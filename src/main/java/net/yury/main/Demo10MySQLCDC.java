package net.yury.main;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import net.yury.serialize.DBDeserialization;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class Demo10MySQLCDC {
    public static void main(String[] args) throws Exception {
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String username = args[2];
        String password = args[3];
        String database = args[4];
        String tableList = "*";
        try{
            tableList = args[5];
        }catch (Exception ignored){ }
        System.out.println(host + " " + port + " " + username + " " + password + " " + database + " " + tableList);

        // 构建Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        // 开启checkpoint
//        env.setStateBackend(new FsStateBackend("hdfs://192.168.141.141:9000/flinkcdc/ck"));
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);

        // 通过FlnkCDC构建SourceFunctin
        MySqlSource.Builder<ObjectNode> builder = MySqlSource.<ObjectNode>builder()
                .hostname(host)
                .port(port)
                .username(username)
                .password(password)
                .databaseList(database);
        if (!"*".equals(tableList)){
            builder.tableList(tableList); // 如果只监控某张表，表明一定要用库名+表名全称
        }
        SourceFunction<ObjectNode> dataSource = builder.serverTimeZone("UTC")
                .startupOptions(StartupOptions.latest())
                .deserializer(new DBDeserialization())
                .build();

        env.addSource(dataSource)
                .print();
//                .addSink(KafkaUtil.getKafkaProducer("192.168.141.141:9092", "change_data_topic"));
        env.execute("FlinkCDC");
        System.out.println("started!");
    }
}
