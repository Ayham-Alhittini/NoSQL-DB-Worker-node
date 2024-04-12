package com.atypon.decentraldbcluster.cache;

import java.util.Optional;

public interface Cache<Key, Value> {
    Optional<Value> get(Key key);
    void put(Key key, Value value);
    void remove(Key key);
    boolean containsKey(Key key);
}
