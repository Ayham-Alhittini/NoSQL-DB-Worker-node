package com.atypon.decentraldbcluster.entity;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.Serial;
import java.io.Serializable;
import java.util.UUID;

public class Document implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    private String id = UUID.randomUUID().toString();
    private JsonNode content;
    private int version = 1;
    private int affinityPort;

    public Document() {}

    public Document(JsonNode data, int affinityPort) {
        this.content = data;
        this.affinityPort = affinityPort;
    }

    public Document(Document src) {
        this.id = src.id;
        this.content = src.getContent();
        this.version = src.getVersion();
        this.affinityPort = src.getAffinityPort();
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
    public int getAffinityPort() {
        return affinityPort;
    }

}