package net.yury.serialize;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import lombok.extern.slf4j.Slf4j;
import net.yury.utils.DateUtil;
import net.yury.utils.JsonUtil;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.source.SourceRecord;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Slf4j
public class DBDeserialization implements DebeziumDeserializationSchema<ObjectNode> {
    public static final DBDeserialization instance = new DBDeserialization();

    public final static String READ = "r";
    public final static String UPDATE = "u";
    public final static String INSERT = "c";
    public final static String DELETE = "d";
    public final static String TRUNCATE = "t";

    public final static String BEFORE = "before";
    public final static String AFTER = "after";

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<ObjectNode> collector) throws Exception {
        ObjectNode node = JsonUtil.getMapper().createObjectNode();
        Struct value = (Struct)sourceRecord.value();
        Struct source = (Struct) value.get("source");
        String database = source.getString("db");
        String schemaName = source.getString("schema");
        String tableName = source.getString("table");
        Long modifyTime = source.getInt64("ts_ms");
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        node.put("mt", modifyTime);          // modify time
        node.put("db", database);            // database
        node.put("sm", schemaName);          // schemaName
        node.put("tb", tableName);           // table name
        node.put("op", operation.code());    // operation

        ObjectNode before = parseNode(value, BEFORE);
        ObjectNode after = parseNode(value, AFTER);
        ArrayNode diffField = parseDiff(before, after, operation.code());
        node.set(BEFORE, before);
        node.set(AFTER, after);
        node.set("diffField", diffField);
        collector.collect(node);
    }

    @Override
    public TypeInformation<ObjectNode> getProducedType() {
        return TypeInformation.of(ObjectNode.class);
    }

    public static ObjectNode parseNode(Struct struct, String field) {
        Object o = struct.get(field);
        if (o == null) {
            return null;
        }
        Struct v = (Struct)o;
        ObjectNode node = JsonUtil.getMapper().createObjectNode();
        List<Field> fields = v.schema().fields();
        for (Field f : fields) {
            Schema.Type type = f.schema().type();
            String fName = f.name();
            switch (type) {
                case INT8:
                    node.put(fName, v.getInt8(fName));
                    break;
                case INT16:
                    node.put(fName, v.getInt16(fName));
                    break;
                case INT32:
                    Integer int32 = v.getInt32(fName);
                    if (int32 == null) {
                        node.set(fName, NullNode.instance);
                        break;
                    }
                    if (io.debezium.time.Date.SCHEMA_NAME.equals(f.schema().name())) {
                        node.put(fName, DateUtil.formatToString(new java.util.Date(int32 * DateUtil.ONE_DAY_SEC)));
                    }else {
                        node.put(fName, int32);
                    }
                    break;
                case INT64:
                    node.put(fName, v.getInt64(fName));
                    break;
                case FLOAT32:
                    node.put(fName, v.getFloat32(fName));
                    break;
                case FLOAT64:
                    node.put(fName, v.getFloat64(fName));
                    break;
                case STRING:
                    node.put(fName, v.getString(fName));
                    break;
                case BOOLEAN:
                    node.put(fName, v.getBoolean(fName));
                    break;
                case BYTES:
                    Object o1 = v.get(fName);
                    if (o1 instanceof BigDecimal) {
                        node.put(fName, (BigDecimal)o1);
                        break;
                    }else if (o1 instanceof byte[]) {
                        node.put(fName, (byte[]) o1);
                        break;
                    }else {
                        log.error("unsupported type [{}], we parse them as string.", f.schema().name());
                        node.put(fName, String.valueOf(o1));
                        break;
                    }
                default:
                    log.error("unsupported type [{}], we parse them as string.", type.getName());
                    node.put(fName, String.valueOf(v.get(fName)));
                    break;
            }
        }
        return node;
    }

    public static ArrayNode parseDiff(ObjectNode before, ObjectNode after, String operation) {
        if (UPDATE.equals(operation)) {
            ArrayNode diff = JsonUtil.getMapper().createArrayNode();
            Iterator<Map.Entry<String, JsonNode>> nodeEntries = before.fields();
            while (nodeEntries.hasNext()) {
                Map.Entry<String, JsonNode> next = nodeEntries.next();
                String f = next.getKey();
                JsonNode v1 = next.getValue();
                JsonNode v2 = after.get(f);
                if ((v1 == null && v2 != null) || (v1 != null && (!v1.equals(v2)))) {
                    diff.add(f);
                }
            }
            return diff;
        }
        return null;
    }
}
