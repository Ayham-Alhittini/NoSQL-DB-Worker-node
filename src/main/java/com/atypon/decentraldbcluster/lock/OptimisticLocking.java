package com.atypon.decentraldbcluster.lock;

import com.atypon.decentraldbcluster.entity.Document;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class OptimisticLocking {
    private final ConcurrentHashMap<String, AtomicInteger> documentVersions = new ConcurrentHashMap<>();

    public boolean attemptVersionUpdate(Document document, int expectedVersion) {
        AtomicInteger version = getVersion(document);
        return version.compareAndSet(expectedVersion, expectedVersion + 1);
    }
    public void clearDocumentVersion(String documentId) {
        documentVersions.remove(documentId);
    }

    private AtomicInteger getVersion(Document document) {
        return documentVersions.computeIfAbsent(document.getId(), ignored -> new AtomicInteger(document.getVersion()));
    }
}
