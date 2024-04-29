package com.atypon.decentraldbcluster.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.UUID;

public class Document {
    private final String DOCUMENT_KEY_NAME = "object_id";
    private JsonNode content;
    private int version = 1;
    private int nodeAffinityPort;

    public Document() {}

    public Document(JsonNode content, int nodeAffinityPort) {
        String id = UUID.randomUUID().toString();
        this.nodeAffinityPort = nodeAffinityPort;
        this.content = appendIdToContent(content, id);
    }

    // To clone exists document on broadcast node
    public Document(JsonNode content, String id, int nodeAffinityPort) {
        this.content = appendIdToContent(content, id);
        this.nodeAffinityPort = nodeAffinityPort;
    }

    @JsonIgnore
    public String getId() {
        return content.get(DOCUMENT_KEY_NAME).asText();
    }

    public JsonNode getContent() {
        return content;
    }

    public void setContent(JsonNode content) {
        this.content = content;
    }

    public int getVersion() {
        return version;
    }

    public void incrementVersion() {
        this.version++;
    }

    public JsonNode appendIdToContent(JsonNode content, String id) {
        ObjectNode temp = new ObjectMapper().createObjectNode();
        temp.put(DOCUMENT_KEY_NAME, id);
        var fields = content.fields();
        while (fields.hasNext()) {
            var field = fields.next();
            if (!DOCUMENT_KEY_NAME.equals(field.getKey()))
                temp.set(field.getKey(), field.getValue());
        }
        return temp;
    }

    public int getNodeAffinityPort() {
        return nodeAffinityPort;
    }
}