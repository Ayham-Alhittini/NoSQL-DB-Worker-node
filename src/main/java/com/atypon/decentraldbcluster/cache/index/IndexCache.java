package com.atypon.decentraldbcluster.cache.index;

import com.atypon.decentraldbcluster.cache.core.Cache;
import com.atypon.decentraldbcluster.cache.core.LRUCache;
import com.atypon.decentraldbcluster.index.Index;
import org.springframework.stereotype.Component;

@Component
public class IndexCache {

    private final Cache<String, Index> cache = new LRUCache<>();

    public Index getIndex(String indexPath) {
        return cache.get(indexPath);
    }

    public void putIndex(Index index, String indexPath) {
        cache.put(indexPath, index);
    }

    public void removeIndex(String indexPath) {
        cache.remove(indexPath);
    }

    public boolean isIndexCached(String indexPath) {
        return cache.containsKey(indexPath);
    }

}