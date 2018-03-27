package com.intrence.cdg.util;

import com.fasterxml.jackson.databind.JsonNode;

public class JsonNodeUtil {

    public static String getTextual(JsonNode root, String key) {
        JsonNode node = root.at(key);
        if (node == null || node.isMissingNode() || !node.isTextual()) {
            throw new IllegalArgumentException(key + " is missing or non-textual");
        }
        return node.asText();
    }

    public static String getTextual(JsonNode root, String key, String defaultValue) {
        JsonNode node = root.at(key);
        if (node == null || node.isMissingNode() || !node.isTextual()) {
            return defaultValue;
        }
        return node.asText();
    }

    public static JsonNode getArray(JsonNode root, String key) {
        JsonNode node = root.at(key);
        if (node == null || node.isMissingNode() || !node.isArray()) {
            throw new IllegalArgumentException(key + " is missing or non-array");
        }
        return node;
    }

    public static int getNumber(JsonNode root, String key) {
        JsonNode node = root.get(key);
        if (node == null || node.isMissingNode() || !node.isNumber()) {
            throw new IllegalArgumentException(key + " is missing or non-numerical");
        }
        return node.asInt();
    }
}
