package com.atypon.decentraldbcluster.entity;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.Serial;
import java.io.Serializable;
import java.util.UUID;

public class Document implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    private String id;
    private JsonNode data;
    private int version;

    public Document() {}

    public Document(JsonNode data) {
        this.id = UUID.randomUUID().toString();
        this.data = data;
        this.version = 1;
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

    public void setVersion(int version) {
        this.version = version;
    }
}