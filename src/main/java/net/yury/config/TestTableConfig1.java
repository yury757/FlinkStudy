package net.yury.config;

import net.yury.entity.FieldInfo;
import net.yury.entity.TableInfo;

import java.util.*;

public class TestTableConfig1 {
    private static Map<Integer, String> CONFIGS = new HashMap<>();

    static {
        FieldInfo fieldInfo1 = new FieldInfo("A", Integer.class);
        FieldInfo fieldInfo2 = new FieldInfo("B", Integer.class);
        List<FieldInfo> fieldInfoList1 = Arrays.<FieldInfo>asList(fieldInfo1, fieldInfo2);
        TableInfo tableInfo1 = new TableInfo("server1", "test", "test1", fieldInfoList1);
        CONFIGS.put(tableInfo1.hashCode(), generateSQL(tableInfo1));

        FieldInfo fieldInfo3 = new FieldInfo("column4", Integer.class);
        FieldInfo fieldInfo4 = new FieldInfo("column5", Integer.class);
        List<FieldInfo> fieldInfoList2 = Arrays.<FieldInfo>asList(fieldInfo3, fieldInfo4);
        TableInfo tableInfo2 = new TableInfo("server1", "test", "test3", fieldInfoList2);
        CONFIGS.put(tableInfo2.hashCode(), generateSQL(tableInfo2));
        System.out.println(CONFIGS);
    }

    public static String getSQL(Integer hashCode) {
        return CONFIGS.get(hashCode);
    }

    private static String generateSQL(TableInfo tableInfo) {
        StringBuilder sql = new StringBuilder("insert into `");
        sql.append(tableInfo.getDatabase());
        sql.append("`.`");
        sql.append(tableInfo.getTableName());
        sql.append("`(");
        for (FieldInfo field : tableInfo.getFields()) {
            sql.append("`");
            sql.append(field.getFieldName());
            sql.append("`,");
        }
        sql.deleteCharAt(sql.length() - 1);
        sql.append(") values (");
        for (int i = 0; i < tableInfo.getFields().size(); i++) {
            sql.append("?,");
        }
        sql.deleteCharAt(sql.length() - 1);
        sql.append(") on duplicate key update ");
        for (FieldInfo field : tableInfo.getFields()) {
            sql.append("`");
            sql.append(field.getFieldName());
            sql.append("` = ");
            sql.append("values(`");
            sql.append(field.getFieldName());
            sql.append("`),");
        }
        sql.deleteCharAt(sql.length() - 1);
        String s = sql.toString();
        System.out.println(s);
        return s;
    }

    public static void main(String[] args) {

    }
}
