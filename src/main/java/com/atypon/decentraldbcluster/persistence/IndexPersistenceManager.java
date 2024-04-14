package com.atypon.decentraldbcluster.persistence;


import com.atypon.decentraldbcluster.cache.index.IndexCache;
import com.atypon.decentraldbcluster.disk.FileSystemService;
import com.atypon.decentraldbcluster.index.Index;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class IndexPersistenceManager {
    private final IndexCache indexCache;
    private final FileSystemService fileSystemService;

    public IndexPersistenceManager(FileSystemService fileSystemService, IndexCache indexCache) {
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

}
