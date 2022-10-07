package net.yury.utils;

import com.fasterxml.jackson.databind.ObjectMapper;

public enum JsonUtil {
    INSTANCE;
    private final ObjectMapper ObjectMapper;
    private JsonUtil() {
        this.ObjectMapper = new ObjectMapper();
    }
    public static ObjectMapper getMapper() {
        return INSTANCE.ObjectMapper;
    }
}
