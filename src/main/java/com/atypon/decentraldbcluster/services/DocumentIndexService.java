package com.atypon.decentraldbcluster.services;

import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.index.Index;
import com.atypon.decentraldbcluster.index.IndexManager;
import com.atypon.decentraldbcluster.utility.PathConstructor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@Service
public class DocumentIndexService {

    private final ObjectMapper mapper;
    private final IndexManager indexManager;
    private final DocumentReaderService documentReaderService;

    @Autowired
    public DocumentIndexService(DocumentReaderService documentService, IndexManager indexManager, ObjectMapper mapper) {
        this.mapper = mapper;
        this.indexManager = indexManager;
        this.documentReaderService = documentService;
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
        indexManager.saveIndex(index, indexPath);
    }

    public void createSystemIdIndex(String collectionPath) throws Exception {
        List<Document> documents = documentReaderService.readDocumentsByCollectionPath(collectionPath);
        String indexPath = PathConstructor.constructSystemGeneratedIndexPath(collectionPath);
        Index index = new Index();
        for (Document document : documents) {
            String documentPath = PathConstructor.constructDocumentPath(collectionPath, document.getId());
            JsonNode key = parseStringToJsonNode(document.getId());
            index.addPointer(key, documentPath);
        }
        indexManager.saveIndex(index, indexPath);
    }

    public void deleteDocumentFromIndexes(String documentPointer) throws Exception {
        Document document = documentReaderService.readDocument(documentPointer);
        String collectionPath = PathConstructor.extractCollectionPathFromDocumentPath(documentPointer);
        List<String> indexedFields = getIndexedFields(document.getContent(), collectionPath);

        for (String field : indexedFields) {
            String indexPath = PathConstructor.constructUserGeneratedIndexPath(collectionPath, field);
            indexManager.removeFromIndex(indexPath, document.getContent().get(field), documentPointer);
        }

        // System-generated index update
        String systemIndexPath = PathConstructor.constructSystemGeneratedIndexPath(collectionPath);
        indexManager.removeFromIndex(systemIndexPath, parseStringToJsonNode(document.getId() ), documentPointer);
    }

    public void updateIndexes(Document document, JsonNode requestBody, String collectionPath) throws Exception {
        List<String> indexedFields = getIndexedFields(requestBody, collectionPath);
        for (String field : indexedFields) {
            String indexPath = PathConstructor.constructUserGeneratedIndexPath(collectionPath, field);
            Index index = indexManager.loadIndex(indexPath);
            JsonNode oldKey = document.getContent().get(field);
            JsonNode newKey = requestBody.get(field);
            String documentPath = PathConstructor.constructDocumentPath(collectionPath, document.getId());

            if (!oldKey.equals(newKey)) { // Only update if the key has changed
                index.removePointer(oldKey, documentPath);
                index.addPointer(newKey, documentPath);
                indexManager.saveIndex(index, indexPath);
            }
        }
    }

    public void insertToAllDocumentIndexes(Document document, String pointer) throws Exception {
        String collectionPath = PathConstructor.extractCollectionPathFromDocumentPath(pointer);
        List<String> indexedFields = getIndexedFields(document.getContent(), collectionPath);

        for (String field : indexedFields) {
            String indexPath = PathConstructor.constructUserGeneratedIndexPath(collectionPath, field);
            JsonNode key = document.getContent().get(field);
            indexManager.addToIndex(indexPath, key, pointer);
        }

        // System-generated ID index update
        insertToSystemIdIndex(document, collectionPath, pointer);
    }

    private void insertToSystemIdIndex(Document document, String collectionPath, String pointer) throws Exception {
        String indexPath = PathConstructor.constructSystemGeneratedIndexPath(collectionPath);
        JsonNode key = parseStringToJsonNode(document.getId());
        indexManager.addToIndex(indexPath, key, pointer);
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

    public JsonNode parseStringToJsonNode(String string) throws IOException {
        return mapper.readTree("\"" + string + "\"");
    }
}
