package com.atypon.decentraldbcluster.services;

import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.index.Index;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@Service
public class IndexService {

    private final DocumentService documentService;
    private final ObjectMapper mapper;

    @Autowired
    public IndexService(DocumentService documentService, ObjectMapper mapper) {
        this.documentService = documentService;
        this.mapper = mapper;
    }

    public String constructUserGeneratedIndexPath(String collectionPath, String fieldName) {
        return Paths.get(collectionPath, "indexes", "user_generated_indexes", fieldName + ".ser").toString();
    }

    public String constructSystemGeneratedIndexPath(String collectionPath) {
        return Paths.get(collectionPath, "indexes", "system_generated_indexes", "id.ser").toString();
    }

    private void addToIndex(String indexPath, JsonNode key, String valuePath) throws Exception {
        Index index = loadIndex(indexPath);
        index.add(key, valuePath);
        saveIndex(index, indexPath);
    }

    private void removeFromIndex(String indexPath, JsonNode key, String valuePath) throws Exception {
        Index index = loadIndex(indexPath);
        index.remove(key, valuePath);
        saveIndex(index, indexPath);
    }

    public Index loadIndex(String indexPath) throws IOException, ClassNotFoundException {
        try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(indexPath))) {
            return (Index) in.readObject();
        } catch (FileNotFoundException e) {
            return new Index(); // Return a new index if file does not exist
        }
    }

    private void saveIndex(Index index, String indexPath) throws IOException {
        try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(indexPath))) {
            out.writeObject(index);
        }
    }

    public void createIndex(String collectionPath, String field) throws Exception {
        List<Document> documents = documentService.readDocumentsByCollectionPath(collectionPath);
        String indexPath = constructUserGeneratedIndexPath(collectionPath, field);
        Index index = new Index();
        for (Document document : documents) {
            String documentPath = documentService.constructDocumentPath(collectionPath, document.getId());
            JsonNode key = document.getData().get(field);
            index.add(key, documentPath);
        }
    }

    public void createSystemIdIndex(String collectionPath) throws Exception {
        List<Document> documents = documentService.readDocumentsByCollectionPath(collectionPath);
        String indexPath = constructSystemGeneratedIndexPath(collectionPath);
        Index index = new Index();
        for (Document document : documents) {
            String documentPath = documentService.constructDocumentPath(collectionPath, document.getId());
            JsonNode key = parseStringToJsonNode(document.getId());
            index.add(key, documentPath);
        }
    }

    public void deleteDocumentFromIndexes(String documentPointer) throws Exception {
        Document document = documentService.readDocument(documentPointer);
        String collectionPath = documentService.getCollectionPathFromDocumentPath(documentPointer);
        List<String> indexedFields = getIndexedFields(document.getData(), collectionPath);

        for (String field : indexedFields) {
            String indexPath = constructUserGeneratedIndexPath(collectionPath, field);
            removeFromIndex(indexPath, document.getData().get(field), documentPointer);
        }

        // System-generated index update
        String systemIndexPath = constructSystemGeneratedIndexPath(collectionPath);
        removeFromIndex(systemIndexPath, mapper.readTree("\"" + document.getId() + "\""), documentPointer);
    }

    public List<String> getIndexedFields(JsonNode jsonNode, String collectionPath) {
        List<String> indexedFields = new ArrayList<>();
        jsonNode.fields().forEachRemaining(field -> {
            String indexPath = constructUserGeneratedIndexPath(collectionPath, field.getKey());
            if (Files.exists(Paths.get(indexPath))) {
                indexedFields.add(field.getKey());
            }
        });
        return indexedFields;
    }

    public void updateIndexes(Document document, JsonNode requestBody, String collectionPath) throws Exception {
        List<String> indexedFields = getIndexedFields(requestBody, collectionPath);
        for (String field : indexedFields) {
            // Update existing index
            String indexPath = constructUserGeneratedIndexPath(collectionPath, field);
            Index index = loadIndex(indexPath);
            JsonNode oldKey = document.getData().get(field);
            JsonNode newKey = requestBody.get(field);
            String documentPath = documentService.constructDocumentPath(collectionPath, document.getId());

            if (!oldKey.equals(newKey)) { // Only update if the key has changed
                index.remove(oldKey, documentPath);
                index.add(newKey, documentPath);
                saveIndex(index, indexPath);
            }
        }
    }

    public void insertToAllIndexes(Document document, String pointer) throws Exception {
        String collectionPath = documentService.getCollectionPathFromDocumentPath(pointer);
        List<String> indexedFields = getIndexedFields(document.getData(), collectionPath);

        for (String field : indexedFields) {
            String indexPath = constructUserGeneratedIndexPath(collectionPath, field);
            JsonNode key = document.getData().get(field);
            addToIndex(indexPath, key, pointer);
        }

        // System-generated ID index update
        insertToSystemIdIndex(document, collectionPath, pointer);
    }

    private void insertToSystemIdIndex(Document document, String collectionPath, String pointer) throws Exception {
        String indexPath = constructSystemGeneratedIndexPath(collectionPath);
        JsonNode key = mapper.readTree("\"" + document.getId() + "\"");
        addToIndex(indexPath, key, pointer);
    }

    // Helper methods for constructing paths, serializing/deserializing indexes, and adding/removing documents from indexes...

    public JsonNode parseStringToJsonNode(String string) throws IOException {
        return mapper.readTree("\"" + string + "\"");
    }
}
