package com.atypon.decentraldbcluster.services;

import com.atypon.decentraldbcluster.entity.Document;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class DocumentVersionManager {
    private final ConcurrentHashMap<String, AtomicInteger> documentVersions = new ConcurrentHashMap<>();


    public boolean updateVersion(Document document, int expectedVersion) {
        AtomicInteger version = getVersion(document);
        return version.compareAndSet(expectedVersion, expectedVersion + 1);
    }

    private AtomicInteger getVersion(Document document) {
        return documentVersions.computeIfAbsent(document.getId(), ignored -> new AtomicInteger(document.getVersion()));
    }
}
