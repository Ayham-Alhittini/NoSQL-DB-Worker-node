package com.atypon.decentraldbcluster.query.index;

import com.atypon.decentraldbcluster.services.DocumentIndexService;
import com.atypon.decentraldbcluster.disk.FileSystemService;
import com.atypon.decentraldbcluster.utility.PathConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class IndexHandler {
    private final FileSystemService fileSystemService;
    private final DocumentIndexService documentIndexService;

    @Autowired
    public IndexHandler(DocumentIndexService documentIndexService, FileSystemService fileSystemService) {
        this.fileSystemService = fileSystemService;
        this.documentIndexService = documentIndexService;
    }

    // TODO: handle field not exists for handleCreateIndex
    public Void handleCreateIndex(IndexQuery query) throws Exception {
        String collectionPath = PathConstructor.constructCollectionPath(query.getOriginator(), query.getDatabase(), query.getCollection());
        documentIndexService.createIndex(collectionPath, query.getField());
        return null;
    }

    public Void handleDropIndex(IndexQuery query) throws IOException {
        String collectionPath = PathConstructor.constructCollectionPath(query.getOriginator(), query.getDatabase(), query.getCollection());
        String indexPath = PathConstructor.constructUserGeneratedIndexPath(collectionPath, query.getField());

        fileSystemService.deleteFile(indexPath);
        return null;
    }
}
