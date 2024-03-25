package com.atypon.decentraldbcluster.index;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.Serial;
import java.io.Serializable;
import java.util.Collections;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class Index implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private final ConcurrentSkipListMap<IndexKey, ConcurrentSkipListSet<String>> indexMap;

    public Index() {
        this.indexMap = new ConcurrentSkipListMap<>();
    }

    public void add(JsonNode jsonNode, String pointer) {
        IndexKey key = new IndexKey(jsonNode);
        if (indexMap.containsKey(key)) {
            var pointers = indexMap.get(key);

            pointers.add(pointer);

            indexMap.put(key, pointers);

        } else {
            indexMap.put(key, new ConcurrentSkipListSet<>(Collections.singleton(pointer)));
        }
    }

    public void remove(JsonNode jsonNode, String pointer) {
        IndexKey key = new IndexKey(jsonNode);
        var pointers = indexMap.get(key);
        pointers.remove(pointer);

        if (pointers.isEmpty())
            indexMap.remove(key);
        else
            indexMap.put(key, pointers);
    }

    public ConcurrentSkipListSet<String> getPointers(JsonNode jsonNode) {
        IndexKey key = new IndexKey(jsonNode);

        if (indexMap.containsKey(key)) {
            return indexMap.get(key);
        }
        return null;
    }

    public boolean containsKey(JsonNode jsonNode) {
        return indexMap.containsKey( new IndexKey(jsonNode) );
    }

}
