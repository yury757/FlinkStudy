package net.yury.sink;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import net.yury.utils.DruidUtil;
import net.yury.utils.JsonUtil;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

public class ModifyLogSink implements SinkFunction<ObjectNode> {
    public static final String statement = "insert into modify_log(modify_time, database, schema_name, table_name, operation, before, after) values (?,?,?,?,?,?,?)";
    public static final ModifyLogSink logSink = new ModifyLogSink();

    @Override
    public void invoke(ObjectNode value) {

    }

    @Override
    public void invoke(ObjectNode value, Context context) throws SQLException, JsonProcessingException {
//        DruidPooledConnection connection = DruidUtil.getConnection();
//        PreparedStatement preparedStatement = connection.prepareStatement(statement);
//        preparedStatement.setTimestamp(1, new Timestamp(value.get("modifyTime").asLong()));
//        preparedStatement.setString(2, value.get("database").asText());
//        preparedStatement.setString(3, value.get("schemaName").asText());
//        preparedStatement.setString(4, value.get("tableName").asText());
//        preparedStatement.setString(5, value.get("operation").asText());
//        preparedStatement.setString(6, JsonUtil.getMapper().writeValueAsString(value.get("before")));
//        preparedStatement.setString(7, JsonUtil.getMapper().writeValueAsString(value.get("after")));
//        preparedStatement.executeUpdate();
//        connection.close();
    }
}
