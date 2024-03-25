package com.atypon.decentraldbcluster.services;

import com.atypon.decentraldbcluster.index.Index;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@Service
public class IndexService {

    private final DocumentService documentService;

    @Autowired
    public IndexService(DocumentService documentService) {
        this.documentService = documentService;
    }

    public String constructIndexPath(String collectionPath, String field) {
        return Paths.get( collectionPath, "indexes", field + ".ser").toString();
    }


    public void createIndex(String collectionPath, String field) throws Exception {

        List<JsonNode> documents = documentService.readDocumentsByCollectionPath(collectionPath);

        String indexPath = constructIndexPath(collectionPath, field);

        Index index = new Index();

        for (var document: documents) {
            String documentId =  document.get("_id").asText();
            String documentPointer = documentService.constructDocumentPath(collectionPath, documentId);

            JsonNode keyNode = document.get(field);

            index.add(keyNode, documentPointer);
        }

        serializeIndex(index, indexPath);
    }

    public void deleteDocumentFromIndexes(String documentPointer) throws Exception {

        JsonNode document = documentService.readDocument(documentPointer);
        String collectionPath = getCollectionPathFromPointer(documentPointer);

        var indexedFields = getIndexedFields(document, collectionPath);

        for (var field: indexedFields) {
            String indexPath = constructIndexPath(collectionPath, field);

            var index = deserializeIndex(indexPath);

            index.remove(document.get(field), documentPointer);

            serializeIndex(index, indexPath);
        }
    }

    public void updateIndexes(JsonNode currentDocument, JsonNode requestBody, String collectionPath) throws Exception {

        String documentPath = documentService.constructDocumentPath(collectionPath, currentDocument.get("_id").asText());
        var indexedFields = getIndexedFields(requestBody, collectionPath);

        for (var field: indexedFields) {

            if (field.equals("_id")) continue;// Prevent update the id

            String indexPath = constructIndexPath(collectionPath, field);

            // Get index
            var index = deserializeIndex(indexPath);

            // Remove document from old key and add it to the new key
            index.remove(currentDocument.get(field), documentPath);
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
            String indexPath = constructIndexPath(collectionPath, field.getKey());
            if (Files.exists(Paths.get(indexPath))) {
                indexedFields.add(field.getKey());
            }
        });

        return indexedFields;
    }

    public void insertToAllIndexes(JsonNode requestBody, String pointer) throws Exception {
        String collectionPath = getCollectionPathFromPointer(pointer);
        var indexedFields = getIndexedFields(requestBody, collectionPath);

        for (var field: indexedFields) {
            var index = deserializeIndex( pointerPathToIndexPath(pointer, field) );

            index.add(requestBody.get(field), pointer);

            serializeIndex(index, constructIndexPath(collectionPath, field) );
        }
    }

    public String getCollectionPathFromPointer(String pointer) {
        return pointer.substring(0, pointer.indexOf("documents"));
    }

    public String pointerPathToIndexPath(String pointer, String field) {
        String collectionPath = getCollectionPathFromPointer(pointer);
        return Paths.get(collectionPath, "indexes", field + ".ser").toString();
    }

}