package com.atypon.decentraldbcluster.entity;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.Serial;
import java.io.Serializable;
import java.util.UUID;

public class Document implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    private final String id = UUID.randomUUID().toString();
    private JsonNode data;
    private int version = 1;
    private int affinityPort;

    public Document() {}

    public Document(JsonNode data, int affinityPort) {
        this.data = data;
        this.affinityPort = affinityPort;
    }

    public String getId() {
        return id;
    }

    public JsonNode getData() {
        return data;
    }

    public void setData(JsonNode data) {
        this.data = data;
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