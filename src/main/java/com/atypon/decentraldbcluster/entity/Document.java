package com.atypon.decentraldbcluster.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.Serial;
import java.io.Serializable;
import java.util.UUID;

public class Document implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    private String id;
    private JsonNode content;
    private int version = 1;

    public Document() {}

    public Document(JsonNode content, int nodeNumber) {
        this.content = content;
        id = UUID.randomUUID().toString() + nodeNumber;
    }

    // To clone exists document on broadcast node
    public Document(JsonNode content, String id) {
        this.id = id;
        this.content = content;
    }

    public String getId() {
        return id;
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

    @JsonIgnore
    public int getAffinityPort() {
        int basePort = 8080;
        // The last digit in the ID represent the node number
        int nodeNumber = id.charAt(id.length() - 1) - '0';
        return basePort + nodeNumber;
    }

}