package com.atypon.decentraldbcluster.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.Serial;
import java.io.Serializable;
import java.util.UUID;

public class Document implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    private JsonNode content;
    private int version = 1;

    public Document() {}

    public Document(JsonNode content, int nodeNumber) {
        String id = UUID.randomUUID().toString() + nodeNumber;
        this.content = appendIdToContent(content, id);
    }

    // To clone exists document on broadcast node
    public Document(JsonNode content, String id) {
        this.content = appendIdToContent(content, id);
    }

    @JsonIgnore
    public String getId() {
        return content.get("object_id").asText();
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
        temp.put("object_id", id);
        var fields = content.fields();
        while (fields.hasNext()) {
            var field = fields.next();
            temp.set(field.getKey(), field.getValue());
        }
        return temp;
    }
}