package com.atypon.decentraldbcluster.services;

import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.index.Index;
import com.fasterxml.jackson.core.JsonProcessingException;
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

    public String constructUserGeneratedIndexesPath(String collectionPath, String field) {
        return Paths.get( collectionPath, "indexes", "user_generated_indexes", field + ".ser").toString();
    }

    public void createIndex(String collectionPath, String field) throws Exception {

        List<Document> documents = documentService.readDocumentsByCollectionPath(collectionPath);

        String indexPath = constructUserGeneratedIndexesPath(collectionPath, field);

        Index index = new Index();

        for (var document: documents) {
            String pointer = documentService.constructDocumentPath(collectionPath, document.getId() );

            JsonNode key = document.getData().get(field);

            index.add(key, pointer);
        }

        serializeIndex(index, indexPath);
    }

    public void createSystemIdIndex(String collectionPath) throws Exception {
        List<Document> documents = documentService.readDocumentsByCollectionPath(collectionPath);

        String indexPath = getSystemGeneratedIdIndexPath(collectionPath);

        Index index = new Index();

        for (var document: documents) {
            String pointer = documentService.constructDocumentPath(collectionPath, document.getId() );

            JsonNode key = parseStringToJsonNode( document.getId() );

            index.add(key, pointer);
        }

        serializeIndex(index, indexPath);

    }

    public void deleteDocumentFromIndexes(String documentPointer) throws Exception {

        Document document = documentService.readDocument(documentPointer);
        String collectionPath = getCollectionPathFromPointer(documentPointer);

        var indexedFields = getIndexedFields(document.getData(), collectionPath);

        for (var field: indexedFields) {
            String indexPath = constructUserGeneratedIndexesPath(collectionPath, field);

            var index = deserializeIndex(indexPath);

            index.remove(document.getData().get(field), documentPointer);

            serializeIndex(index, indexPath);
        }

        deleteFromSystemIdIndex(collectionPath, document.getId(), documentPointer);
    }

    private void deleteFromSystemIdIndex(String collectionPath, String id, String pointer) throws Exception {
        String indexPath = getSystemGeneratedIdIndexPath(collectionPath);
        var index = deserializeIndex(indexPath);

        index.remove( parseStringToJsonNode(id) , pointer );

        serializeIndex(index, indexPath);
    }



    public void updateIndexes(Document document, JsonNode requestBody, String collectionPath) throws Exception {

        String documentPath = documentService.constructDocumentPath(collectionPath, document.getId());
        var indexedFields = getIndexedFields(requestBody, collectionPath);

        for (var field: indexedFields) {

            String indexPath = constructUserGeneratedIndexesPath(collectionPath, field);

            // Get index
            var index = deserializeIndex(indexPath);

            // Remove document from old key and add it to the new key
            index.remove(document.getData().get(field), documentPath);
            index.add(requestBody.get(field), documentPath);

            // Save changes
            serializeIndex(index, indexPath);
        }
    }


    public void serializeIndex(Index index, String indexPath) throws Exception {
        try (FileOutputStream fileOut = new FileOutputStream(indexPath);
             ObjectOutputStream out = new ObjectOutputStream(fileOut)) {
             out.writeObject(index);
        }
    }

    public Index deserializeIndex(String indexPath) throws Exception {

        Index index;

        try (FileInputStream fileIn = new FileInputStream(indexPath);

             ObjectInputStream in = new ObjectInputStream(fileIn)) {
             index = (Index) in.readObject();
        }
        return index;
    }


    public List<String> getIndexedFields(JsonNode jsonNode, String collectionPath) {
        List<String> indexedFields = new ArrayList<>();

        jsonNode.fields().forEachRemaining(field -> {
            String indexPath = constructUserGeneratedIndexesPath(collectionPath, field.getKey());
            if (Files.exists(Paths.get(indexPath))) {
                indexedFields.add(field.getKey());
            }
        });

        return indexedFields;
    }

    public void insertToAllIndexes(Document document, String pointer) throws Exception {
        String collectionPath = getCollectionPathFromPointer(pointer);
        var indexedFields = getIndexedFields(document.getData(), collectionPath);

        for (var field: indexedFields) {

            var index = deserializeIndex( constructUserGeneratedIndexesPath(collectionPath, field) );

            index.add(document.getData().get(field), pointer);

            serializeIndex(index, constructUserGeneratedIndexesPath(collectionPath, field) );
        }

        insertToSystemIdIndex(collectionPath, document.getId(), pointer);
    }

    private void insertToSystemIdIndex(String collectionPath, String id, String pointer) throws Exception {
        String indexPath = getSystemGeneratedIdIndexPath(collectionPath);
        var index = deserializeIndex(indexPath);

        index.add( parseStringToJsonNode(id) , pointer );

        serializeIndex(index, indexPath);
    }

    public String getSystemGeneratedIdIndexPath(String collectionPath) {
        return Paths.get(collectionPath, "indexes", "system_generated_indexes", "id.ser").toString();
    }

    public String getCollectionPathFromPointer(String pointer) {
        return pointer.substring(0, pointer.indexOf("documents"));
    }

    public JsonNode parseStringToJsonNode(String string) throws JsonProcessingException {
        return mapper.readTree( "\"" + string + "\"");
    }

}