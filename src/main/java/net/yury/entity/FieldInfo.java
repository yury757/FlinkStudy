package net.yury.entity;

import lombok.*;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FieldInfo implements Serializable {
    private String fieldName;
    private Class<?> fieldType;
}
