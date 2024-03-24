package com.atypon.decentraldbcluster.entity;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.Serial;
import java.io.Serializable;

public class IndexKey implements Comparable<IndexKey> , Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    public JsonNode object;

    public IndexKey(JsonNode object) {
        this.object = object;
    }



    @Override
    public int compareTo(IndexKey other) {
        return Integer.compare(this.hashCode(), other.hashCode());
    }


    @Override
    public boolean equals(Object o) {
        return this.hashCode() == o.hashCode();
    }

    @Override
    public int hashCode() {
        return object.hashCode();
    }

    @Override
    public String toString() {
        return object.toPrettyString();
    }
}