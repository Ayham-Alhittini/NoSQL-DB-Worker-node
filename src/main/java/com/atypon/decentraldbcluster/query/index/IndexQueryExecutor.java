package com.atypon.decentraldbcluster.query.index;

import com.atypon.decentraldbcluster.query.base.Executable;
import com.atypon.decentraldbcluster.services.DocumentIndexService;
import com.atypon.decentraldbcluster.services.FileSystemService;
import com.atypon.decentraldbcluster.services.PathConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class IndexQueryExecutor implements Executable<IndexQuery> {

    private final DocumentIndexService documentIndexService;
    private final FileSystemService fileSystemService;

    @Autowired
    public IndexQueryExecutor(DocumentIndexService documentIndexService, FileSystemService fileSystemService) {
        this.documentIndexService = documentIndexService;
        this.fileSystemService = fileSystemService;
    }

    @Override
    public Object exec(IndexQuery query) throws Exception {
        return switch (query.getIndexAction()) {
            case CREATE -> handleCreateIndex(query);
            case DROP -> handleDropIndex(query);
        };
    }

    private Object handleCreateIndex(IndexQuery query) throws Exception {
        String collectionPath = PathConstructor.constructCollectionPath(query.getOriginator(), query.getDatabase(), query.getCollection());
        documentIndexService.createIndex(collectionPath, query.getField());
        return null;
    }

    private Object handleDropIndex(IndexQuery query) throws IOException {
        String collectionPath = PathConstructor.constructCollectionPath(query.getOriginator(), query.getDatabase(), query.getCollection());
        String indexPath = PathConstructor.constructUserGeneratedIndexPath(collectionPath, query.getField());

        fileSystemService.deleteFile(indexPath);
        return null;
    }

}


// TODO: handle field not exists for handleCreateIndex
