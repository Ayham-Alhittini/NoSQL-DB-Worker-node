package com.atypon.decentraldbcluster.query.handlers.index;

import com.atypon.decentraldbcluster.document.services.DocumentIndexService;
import com.atypon.decentraldbcluster.persistence.IndexPersistenceManager;
import com.atypon.decentraldbcluster.query.types.IndexQuery;
import com.atypon.decentraldbcluster.utility.PathConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class IndexHandler {
    private final DocumentIndexService documentIndexService;
    private final IndexPersistenceManager indexPersistenceManager;

    @Autowired
    public IndexHandler(DocumentIndexService documentIndexService, IndexPersistenceManager indexPersistenceManager) {
        this.documentIndexService = documentIndexService;
        this.indexPersistenceManager = indexPersistenceManager;
    }

    public Void handleCreateIndex(IndexQuery query) throws Exception {
        String collectionPath = PathConstructor.constructCollectionPath(query.getOriginator(), query.getDatabase(), query.getCollection());
        documentIndexService.createIndex(collectionPath, query.getField());
        return null;
    }

    public Void handleDropIndex(IndexQuery query) throws IOException {
        String collectionPath = PathConstructor.constructCollectionPath(query.getOriginator(), query.getDatabase(), query.getCollection());
        String indexPath = PathConstructor.constructUserGeneratedIndexPath(collectionPath, query.getField());

        indexPersistenceManager.removeIndex(indexPath);
        return null;
    }
}
