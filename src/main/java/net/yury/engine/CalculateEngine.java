package net.yury.engine;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class CalculateEngine {
    private String request;
    public static void calculate(ObjectNode request) {
        String mode = request.get("mode").asText();

    }
}
