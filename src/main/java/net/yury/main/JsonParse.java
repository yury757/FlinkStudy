//package net.yury.main;
//
//import com.alibaba.fastjson.JSONObject;
//import com.alibaba.fastjson.serializer.SerializerFeature;
//
//import java.math.BigDecimal;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
///**
// * 给定一个纯数据json，再给定这个json的字段名，将数据和字段名组装起来，生成一个带字段名格式的json，如：
// * source json为：
// * {
// *   "600000.SH": {
// *     "20201231": {
// *       "10001": 123,
// *       "10002": 234
// *     },
// *     "20191231": {
// *       "10001": 345,
// *       "10002": 456
// *     }
// *   },
// *   "000001.SZ": {
// *     "20000101": {
// *       "10001": null,
// *       "10003": 567
// *     }
// *   }
// * }
// *
// * pattern为：
// * {
// *   "type": "object",
// *   "keyName": "thscode",
// *   "next": {
// *     "type": "object",
// *     "keyName": "date",
// *     "next": {
// *       "type": "object",
// *       "keyName": "itemid",
// *       "next": {
// *         "type": "number",
// *         "keyName": "value",
// *         "next": null
// *       }
// *     }
// *   }
// * }
// *
// * 生成的目标json为：
// * [
// *   {
// *     "date": "20000101",
// *     "itemid": "10001",
// *     "thscode": "000001.SZ",
// *     "value": null
// *   },
// *   {
// *     "date": "20000101",
// *     "itemid": "10003",
// *     "thscode": "000001.SZ",
// *     "value": 567
// *   },
// *   {
// *     "date": "20201231",
// *     "itemid": "10002",
// *     "thscode": "600000.SH",
// *     "value": 234
// *   },
// *   {
// *     "date": "20201231",
// *     "itemid": "10001",
// *     "thscode": "600000.SH",
// *     "value": 123
// *   },
// *   {
// *     "date": "20191231",
// *     "itemid": "10002",
// *     "thscode": "600000.SH",
// *     "value": 456
// *   },
// *   {
// *     "date": "20191231",
// *     "itemid": "10001",
// *     "thscode": "600000.SH",
// *     "value": 345
// *   }
// * ]
// */
//public class JsonParse {
//
//    public static void main(String[] args) {
//
//        String pattern = "{\"type\":\"object\",\"keyName\":\"thscode\",\"next\":{\"type\":\"object\",\"keyName\":\"date\",\"next\":{\"type\":\"object\",\"keyName\":\"itemid\",\"next\":{\"type\":\"number\",\"keyName\":\"value\",\"next\":null}}}}";
//        String value = "{\"600000.SH\":{\"20201231\":{\"10001\":123,\"10002\":234},\"20191231\":{\"10001\":345,\"10002\":456}},\"000001.SZ\":{\"20000101\":{\"10001\":null,\"10003\":567}}}";
//
//        parse(value, pattern);
//    }
//
//    public static void parse(String data, String pattern) {
//        JSONObject patternJson = JSONObject.parseObject(pattern);
//        JSONObject valueJson = JSONObject.parseObject(data);
//
//        List<Map<String, Object>> res = new ArrayList<>();
//        solve(res, null, patternJson, valueJson);
//        String s = JSONObject.toJSONString(res, SerializerFeature.WriteMapNullValue);
//        System.out.println(res);
//        System.out.println(s);
//    }
//
//    public static void solve(List<Map<String, Object>> res, Map<String, Object> tmp, JSONObject pattern, Object value) {
//        if (tmp == null){
//            tmp = new HashMap<>();
//        }
//        String type = pattern.getString("type");
//        if ("number".equals(type)){
//            String stringValue = String.valueOf(value);
//            BigDecimal v;
//            if ("null".equals(stringValue)) {
//                v = null;
//            }else {
//                v = new BigDecimal(stringValue);
//            }
//            tmp.put(pattern.getString("keyName"), v);
//            HashMap<String, Object> resultMap = new HashMap<>(tmp);
//            res.add(resultMap);
//            return;
//        }
//        if ("string".equals(type)){
//            String stringValue = String.valueOf(value);
//            stringValue = "null".equals(stringValue)? null: stringValue;
//            tmp.put(pattern.getString("keyName"), stringValue);
//            HashMap<String, Object> resultMap = new HashMap<>(tmp);
//            res.add(resultMap);
//            return;
//        }
//        if ("boolean".equals(type)){
//            String stringValue = String.valueOf(value);
//            Boolean v;
//            if ("null".equals(stringValue)) {
//                v = null;
//            }else {
//                v = Boolean.valueOf(stringValue);
//            }
//            tmp.put(pattern.getString("keyName"), v);
//            HashMap<String, Object> resultMap = new HashMap<>(tmp);
//            res.add(resultMap);
//            return;
//        }
//        if ("object".equals(type)){
//            JSONObject v = (JSONObject) value;
//            for (String key : v.keySet()) {
//                String keyName = pattern.getString("keyName");
//                tmp.put(keyName, key);
//                solve(res, tmp, pattern.getJSONObject("next"), v.get(key));
//                tmp.remove(keyName);
//            }
//        }
//    }
//}
