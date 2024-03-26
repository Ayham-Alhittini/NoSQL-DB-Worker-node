package com.atypon.decentraldbcluster.index;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.Serial;
import java.io.Serializable;

public class IndexKey implements Comparable<IndexKey> , Serializable {

    @Serial
    private static final long serialVersionUID = 2L;

    public JsonNode indexedField;

    public IndexKey(JsonNode indexedField) {
        this.indexedField = indexedField;
    }



    @Override
    public int compareTo(IndexKey other) {
        return Integer.compare(this.hashCode(), other.hashCode());
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (!(o instanceof IndexKey other)) return false;

        return indexedField.equals(other.indexedField);
    }


    @Override
    public int hashCode() {
        return indexedField.hashCode();
    }

    @Override
    public String toString() {
        return indexedField.toPrettyString();
    }
}