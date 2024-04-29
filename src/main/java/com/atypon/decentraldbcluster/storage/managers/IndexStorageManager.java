package com.atypon.decentraldbcluster.storage.managers;


import com.atypon.decentraldbcluster.cache.index.IndexCache;
import com.atypon.decentraldbcluster.storage.disk.FileSystemService;
import com.atypon.decentraldbcluster.index.Index;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class IndexStorageManager {
    private final IndexCache indexCache;
    private final FileSystemService fileSystemService;

    public IndexStorageManager(FileSystemService fileSystemService, IndexCache indexCache) {
        this.fileSystemService = fileSystemService;
        this.indexCache = indexCache;
    }

    public void saveIndex(String indexPath, Index index) throws IOException {
        fileSystemService.saveIndex(index, indexPath);
        indexCache.putIndex(index, indexPath);
    }

    public Index loadIndex(String indexPath) throws Exception {
        if (indexCache.isIndexCached(indexPath)) {
            return indexCache.getIndex(indexPath);
        }
        Index index = fileSystemService.loadIndex(indexPath);
        indexCache.putIndex(index, indexPath);
        return index;
    }

    public void removeIndex(String indexPath) throws IOException {
        fileSystemService.deleteFile(indexPath);
        indexCache.removeIndex(indexPath);
    }

    public void addToIndex(String indexPath, JsonNode key, String valuePath) throws Exception {
        Index index = loadIndex(indexPath);
        index.addPointer(key, valuePath);
        saveIndex(indexPath, index);
    }

    public void removeFromIndex(String indexPath, JsonNode key, String valuePath) throws Exception {
        Index index = loadIndex(indexPath);
        index.removePointer(key, valuePath);
        saveIndex(indexPath, index);
    }

}
