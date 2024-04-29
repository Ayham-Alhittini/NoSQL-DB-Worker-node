package com.atypon.decentraldbcluster.cache.datastructure;

public interface Cache<Key, Value> {
    Value get(Key key);
    void put(Key key, Value value);
    void remove(Key key);
    boolean containsKey(Key key);
    int size();
}
