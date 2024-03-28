package com.atypon.decentraldbcluster.services;

import com.atypon.decentraldbcluster.index.Index;
import com.atypon.decentraldbcluster.lock.DiskResourcesLock;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;

@Service
public class IndexManager {

    private final DiskResourcesLock resourcesLock;

    @Autowired
    public IndexManager(DiskResourcesLock resourcesLock) {
        this.resourcesLock = resourcesLock;
    }

    public Index loadIndex(String indexPath) throws Exception {
        resourcesLock.lockReadResource(indexPath);
        try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(indexPath))) {
            return (Index) in.readObject();
        } finally {
            resourcesLock.releaseReadResource(indexPath);
        }
    }

    public void saveIndex(Index index, String indexPath) throws IOException {
        resourcesLock.lockWriteResource(indexPath);
        try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(indexPath))) {
            out.writeObject(index);
        } finally {
            resourcesLock.releaseWriteResource(indexPath);
        }
    }

    public void addToIndex(String indexPath, JsonNode key, String valuePath) throws Exception {
        Index index = loadIndex(indexPath);
        index.add(key, valuePath);
        saveIndex(index, indexPath);
    }

    public void removeFromIndex(String indexPath, JsonNode key, String valuePath) throws Exception {
        Index index = loadIndex(indexPath);
        index.remove(key, valuePath);
        saveIndex(index, indexPath);
    }
}
