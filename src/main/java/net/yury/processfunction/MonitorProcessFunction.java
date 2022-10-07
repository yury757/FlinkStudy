package net.yury.processfunction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import net.yury.main.Demo12monitor;
import net.yury.serialize.DBDeserialization;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

public class MonitorProcessFunction extends ProcessFunction<ObjectNode, ObjectNode> {
    public static final MonitorProcessFunction function = new MonitorProcessFunction();
    @Override
    public void processElement(ObjectNode value, Context ctx, Collector<ObjectNode> out) throws Exception {
        // System.out.println("raw data: " + value.toString());
        String op = value.get("operation").asText();
        if (DBDeserialization.DELETE.equals(op)
                || DBDeserialization.TRUNCATE.equals(op)
                || DBDeserialization.READ.equals(op)) {
            return;
        }
        JsonNode a = value.get(DBDeserialization.AFTER);
        if (a instanceof NullNode) {
            return;
        }
        ObjectNode after = (ObjectNode)a;
        String tableName = value.get("tableName").asText();
        if (!Demo12monitor.config.containsKey(tableName)) {
            return;
        }
        Map<String, List<Demo12monitor.Rule>> tableRule = Demo12monitor.config.get(tableName);
        if (DBDeserialization.INSERT.equals(op)) {
            for (Map.Entry<String, List<Demo12monitor.Rule>> ruleEntry : tableRule.entrySet()) {
                String field = ruleEntry.getKey();
                double v;
                try {
                    v = after.get(field).asDouble();
                }catch (Exception ex) {
                    continue;
                }
                List<Demo12monitor.Rule> rules = ruleEntry.getValue();
                for (Demo12monitor.Rule rule : rules) {
                    if (rule.decide(v)) {
                        out.collect(value);
                    }
                }
            }
        }else {
            JsonNode diff = value.get("diffField");
            if (diff instanceof NullNode || diff.size() == 0) {
                return;
            }
            for (JsonNode fieldNode : (ArrayNode) diff) {
                String field = fieldNode.asText();
                List<Demo12monitor.Rule> ruleList = tableRule.getOrDefault(field, null);
                if (ruleList == null) {
                    continue;
                }
                for (Demo12monitor.Rule rule : ruleList) {
                    double v;
                    try {
                        v = after.get(field).asDouble();
                    }catch (Exception ex) {
                        continue;
                    }
                    if (rule.decide(v)) {
                        out.collect(value);
                    }
                }
            }
        }
    }
}
