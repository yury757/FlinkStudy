package net.yury.entity;

import lombok.*;

import java.io.Serializable;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableInfo implements Serializable {
    private String server;
    private String database;
    private String tableName;
    private List<FieldInfo> fields;
}
