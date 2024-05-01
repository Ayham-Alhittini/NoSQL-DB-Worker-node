package com.atypon.decentraldbcluster.query.handlers.index;

import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.index.Index;
import com.atypon.decentraldbcluster.storage.disk.FileSystemService;
import com.atypon.decentraldbcluster.storage.managers.DocumentStorageManager;
import com.atypon.decentraldbcluster.storage.managers.IndexStorageManager;
import com.atypon.decentraldbcluster.query.types.IndexQuery;
import com.atypon.decentraldbcluster.utility.PathConstructor;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

@Service
public class IndexHandler {
    private final FileSystemService fileSystemService;
    private final IndexStorageManager indexStorageManager;
    private final DocumentStorageManager documentStorageManager;

    @Autowired
    public IndexHandler(FileSystemService fileSystemService, IndexStorageManager indexPersistenceManager, DocumentStorageManager documentStorageManager) {
        this.fileSystemService = fileSystemService;
        this.indexStorageManager = indexPersistenceManager;
        this.documentStorageManager = documentStorageManager;
    }

    public Void handleCreateIndex(IndexQuery query) throws Exception {
        String collectionPath = PathConstructor.constructCollectionPath(query.getOriginator(), query.getDatabase(), query.getCollection());
        createIndex(collectionPath, query.getField());
        return null;
    }

    public Void handleDropIndex(IndexQuery query) throws IOException {
        String collectionPath = PathConstructor.constructCollectionPath(query);
        String indexPath = PathConstructor.constructIndexPath(collectionPath, query.getField());

        indexStorageManager.removeIndex(indexPath);
        return null;
    }


    public List<String> handleShowIndexes(IndexQuery query) {
        String collectionPath = PathConstructor.constructCollectionPath(query);
        String indexesPath = Path.of(collectionPath, "indexes").toString();
        return fileSystemService.getDirectoryFilesPath(indexesPath)
                .stream().map(path -> path.substring(path.indexOf("indexes\\") + "indexes\\".length()))
                .toList();
    }


    public void createIndex(String collectionPath, String field) throws Exception {
        List<Document> documents = documentStorageManager.getCollectionDocuments(collectionPath);
        String indexPath = PathConstructor.constructIndexPath(collectionPath, field);
        Index index = new Index();
        for (Document document : documents) {
            String documentPath = PathConstructor.constructDocumentPath(collectionPath, document.getId());
            if (document.getContent().has(field)) {
                JsonNode key = document.getContent().get(field);
                index.addPointer(key, documentPath);
            }
        }
        indexStorageManager.saveIndex(indexPath, index);
    }
}
