package com.atypon.decentraldbcluster.document.services;

import com.atypon.decentraldbcluster.document.entity.Document;
import com.atypon.decentraldbcluster.index.Index;
import com.atypon.decentraldbcluster.persistence.DocumentPersistenceManager;
import com.atypon.decentraldbcluster.persistence.IndexPersistenceManager;
import com.atypon.decentraldbcluster.utility.PathConstructor;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@Service
public class DocumentIndexService {
    private final DocumentQueryService documentReaderService;
    private final IndexPersistenceManager indexPersistenceManager;
    private final DocumentPersistenceManager documentPersistenceManager;

    @Autowired
    public DocumentIndexService(DocumentQueryService documentService, IndexPersistenceManager indexPersistenceManager,
                                DocumentPersistenceManager documentPersistenceManager) {
        this.documentReaderService = documentService;
        this.indexPersistenceManager = indexPersistenceManager;
        this.documentPersistenceManager = documentPersistenceManager;
    }

    public void createIndex(String collectionPath, String field) throws Exception {
        List<Document> documents = documentReaderService.readDocumentsByCollectionPath(collectionPath);
        String indexPath = PathConstructor.constructUserGeneratedIndexPath(collectionPath, field);
        Index index = new Index();
        boolean findAtLeastOneDocumentWithTheFiled = false;
        for (Document document : documents) {
            String documentPath = PathConstructor.constructDocumentPath(collectionPath, document.getId());
            if (document.getContent().has(field)) {
                JsonNode key = document.getContent().get(field);
                index.addPointer(key, documentPath);
                findAtLeastOneDocumentWithTheFiled = true;
            }
        }
        if (!findAtLeastOneDocumentWithTheFiled) throw new IllegalArgumentException("Field not exists");
        indexPersistenceManager.saveIndex(indexPath, index);
    }

    public void deleteDocumentFromIndexes(String documentPointer) throws Exception {
        Document document = documentPersistenceManager.loadDocument(documentPointer);
        String collectionPath = PathConstructor.extractCollectionPathFromDocumentPath(documentPointer);
        List<String> indexedFields = getIndexedFields(document.getContent(), collectionPath);

        for (String field : indexedFields) {
            String indexPath = PathConstructor.constructUserGeneratedIndexPath(collectionPath, field);
            removeFromIndex(indexPath, document.getContent().get(field), documentPointer);
        }
    }

    public void updateIndexes(Document document, JsonNode requestBody, String collectionPath) throws Exception {
        List<String> indexedFields = getIndexedFields(requestBody, collectionPath);
        for (String field : indexedFields) {
            String indexPath = PathConstructor.constructUserGeneratedIndexPath(collectionPath, field);
            Index index = indexPersistenceManager.loadIndex(indexPath);
            JsonNode oldKey = document.getContent().get(field);
            JsonNode newKey = requestBody.get(field);
            String documentPath = PathConstructor.constructDocumentPath(collectionPath, document.getId());

            if (!oldKey.equals(newKey)) { // Only update if the key has changed
                index.removePointer(oldKey, documentPath);
                index.addPointer(newKey, documentPath);
                indexPersistenceManager.saveIndex(indexPath, index);
            }
        }
    }

    public void insertToAllDocumentIndexes(Document document, String pointer) throws Exception {
        String collectionPath = PathConstructor.extractCollectionPathFromDocumentPath(pointer);
        List<String> indexedFields = getIndexedFields(document.getContent(), collectionPath);

        for (String field : indexedFields) {
            String indexPath = PathConstructor.constructUserGeneratedIndexPath(collectionPath, field);
            JsonNode key = document.getContent().get(field);
            addToIndex(indexPath, key, pointer);
        }
    }

    public List<String> getIndexedFields(JsonNode jsonNode, String collectionPath) {
        List<String> indexedFields = new ArrayList<>();
        jsonNode.fields().forEachRemaining(field -> {
            String indexPath = PathConstructor.constructUserGeneratedIndexPath(collectionPath, field.getKey());
            if (Files.exists(Paths.get(indexPath))) {
                indexedFields.add(field.getKey());
            }
        });
        return indexedFields;
    }

    public void addToIndex(String indexPath, JsonNode key, String valuePath) throws Exception {
        Index index = indexPersistenceManager.loadIndex(indexPath);
        index.addPointer(key, valuePath);
        indexPersistenceManager.saveIndex(indexPath, index);
    }

    public void removeFromIndex(String indexPath, JsonNode key, String valuePath) throws Exception {
        Index index = indexPersistenceManager.loadIndex(indexPath);
        index.removePointer(key, valuePath);
        indexPersistenceManager.saveIndex(indexPath, index);
    }

}
