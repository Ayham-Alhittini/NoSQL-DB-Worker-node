package com.atypon.decentraldbcluster.cache.document;

import com.atypon.decentraldbcluster.cache.datastructure.Cache;
import com.atypon.decentraldbcluster.cache.datastructure.LRUCache;
import com.atypon.decentraldbcluster.entity.Document;
import org.springframework.stereotype.Component;

@Component
public class DocumentCache {

    private final Cache<String, Document> cache = new LRUCache<>();

    public Document getDocument(String documentPath) {
        return cache.get(documentPath);
    }

    public void putDocument(String documentPath, Document document) {
        cache.put(documentPath, document);
    }

    public void removeDocument(String documentPath) {
        cache.remove(documentPath);
    }

    public boolean isDocumentCached(String documentPath) {
        return cache.containsKey(documentPath);
    }
}
