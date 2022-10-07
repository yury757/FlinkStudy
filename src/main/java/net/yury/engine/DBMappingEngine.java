package net.yury.engine;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import net.yury.utils.DruidUtil;
import net.yury.utils.JsonUtil;
import org.postgresql.jdbc.PgConnection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;

public class DBMappingEngine implements Engine {

    @Override
    public Object calculate(JsonNode request) throws SQLException {
        String tableName = request.get("tableName").asText();
        ArrayNode targetFields = (ArrayNode)request.get("targetField");
        ObjectNode param = (ObjectNode)request.get("param");
        DruidPooledConnection connection = DruidUtil.getConnection();
        PgConnection conn = (PgConnection)connection.getConnection();
        String sql = buildSQL(tableName, targetFields, conn, param);
        System.out.println(sql);
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();
        ArrayNode ans = JsonUtil.getMapper().createArrayNode();
        while (resultSet.next()) {
            ObjectNode row = JsonUtil.getMapper().createObjectNode();
            for (JsonNode targetField : targetFields) {
                row.put(targetField.asText(), resultSet.getString(targetField.asText()));
            }
            ans.add(row);
        }
        return ans;
    }

    public static String buildSQL(String tableName, ArrayNode targetFields, PgConnection connection, ObjectNode param) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("select ");
        for (JsonNode targetField : targetFields) {
            String f = targetField.asText();
            sb.append(f).append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(" from ").append(tableName).append(" where");
        Iterator<Map.Entry<String, JsonNode>> fields = param.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> node = fields.next();
            sb.append(" ").append(node.getKey()).append("='").append(connection.escapeString(node.getValue().asText())).append("' and");
        }
        sb.delete(sb.length() - 4, sb.length());
        return sb.toString();
    }

    public static String prefixSQL(String tableName, ArrayNode targetFields) {
        StringBuilder sb = new StringBuilder();
        sb.append("select ");
        for (JsonNode targetField : targetFields) {
            String f = targetField.asText();
            sb.append(f).append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(" from ").append(tableName).append(" where ");
        return sb.toString();
    }

    public static String buildValue(PgConnection connection, ObjectNode param) throws SQLException {
        StringBuilder sb = new StringBuilder();
        Iterator<Map.Entry<String, JsonNode>> fields = param.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> node = fields.next();
            sb.append(" ").append(node.getKey()).append("=").append(connection.escapeString(node.getValue().asText())).append(" and");
        }
        sb.delete(sb.length() - 4, sb.length());
        return sb.toString();
    }

    public static void main(String[] args) {
        try {
            DBMappingEngine engine = new DBMappingEngine();
//            String req = "{\"mode\":\"DBMapping\",\"tableName\":\"t1\",\"param\":{\"value\":\"a1\",\"counts\":12},\"targetField\":[\"counts\",\"value\"]}";
//            String req = "{\"mode\":\"DBMapping\",\"tableName\":\"t2\",\"param\":{\"name\":\"xiaoming\"},\"targetField\":[\"age\",\"name\",\"address\",\"country\"]}";
//            String req = "{\"mode\":\"DBMapping\",\"tableName\":\"t2\",\"param\":{\"1\":\"1\"},\"targetField\":[\"age\",\"name\",\"address\",\"country\"]}";
            String req = "{\"mode\":\"DBMapping\",\"tableName\":\"t2\",\"param\":{\"country\":\"zhongguo\"},\"targetField\":[\"age\",\"name\",\"address\",\"country\"]}";
            ObjectNode request = (ObjectNode)JsonUtil.getMapper().readTree(req);
            Object ans = engine.calculate(request);
            System.out.println(ans);
        }catch (Exception ex) {
            ex.printStackTrace();
        }finally {
            DruidUtil.shutdown();
        }
    }
}
