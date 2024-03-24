package com.atypon.decentraldbcluster.services;

import com.atypon.decentraldbcluster.entity.IndexKey;
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
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

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

    public Set<String> getPointers(ConcurrentSkipListMap<IndexKey, ConcurrentSkipListSet<String>> index, JsonNode field) {
        IndexKey key = new IndexKey(field);

        if (index.containsKey(key)) {
            return index.get(key);
        }
        return null;
    }

    public void createIndex(String collectionPath, String field) throws Exception {

        List<JsonNode> documents = documentService.readDocumentsByCollectionPath(collectionPath);

        String indexPath = constructIndexPath(collectionPath, field);

        ConcurrentSkipListMap<IndexKey, ConcurrentSkipListSet<String>> index = new ConcurrentSkipListMap<>();

        for (var document: documents) {
            String documentId =  document.get("_id").asText();
            String documentPointer = documentService.constructDocumentPath(collectionPath, documentId);

            JsonNode keyNode = document.get(field);
            IndexKey key = new IndexKey(keyNode);

            addToIndex(index, key, documentPointer);
        }

        serializeIndex(index, indexPath);
    }

    public void deleteDocumentFromIndexes(String documentPath) throws Exception {

        JsonNode document = documentService.readDocument(documentPath);
        String collectionPath = getCollectionPathFromPointer(documentPath);

        var indexedFields = getIndexedFields(document, collectionPath);

        for (var field: indexedFields) {
            String indexPath = constructIndexPath(collectionPath, field);

            var index = deserializeIndex(indexPath);

            removeFromIndex(index, new IndexKey( document.get(field) ), documentPath);

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
            removeFromIndex(index, new IndexKey(currentDocument.get(field)), documentPath);
            addToIndex(index, new IndexKey( requestBody.get(field) ), documentPath);

            // Save changes
            serializeIndex(index, indexPath);
        }
    }


    public void serializeIndex(ConcurrentSkipListMap<IndexKey, ConcurrentSkipListSet<String>> index, String indexPath) throws Exception {
        try (FileOutputStream fileOut = new FileOutputStream(indexPath);
             ObjectOutputStream out = new ObjectOutputStream(fileOut)) {
             out.writeObject(index);
        }
    }

    @SuppressWarnings("unchecked")
    public ConcurrentSkipListMap<IndexKey, ConcurrentSkipListSet<String>> deserializeIndex(String indexPath) throws Exception {

        ConcurrentSkipListMap<IndexKey, ConcurrentSkipListSet<String>> index;

        try (FileInputStream fileIn = new FileInputStream(indexPath);

             ObjectInputStream in = new ObjectInputStream(fileIn)) {
            index = (ConcurrentSkipListMap<IndexKey, ConcurrentSkipListSet<String>>) in.readObject();
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
            var fieldIndex = deserializeIndex( pointerPathToIndexPath(pointer, field) );
            addToIndex(fieldIndex, new IndexKey(requestBody.get(field)), pointer);
            serializeIndex(fieldIndex, constructIndexPath(collectionPath, field) );
        }
    }

    public void addToIndex(ConcurrentSkipListMap<IndexKey, ConcurrentSkipListSet<String>> index, IndexKey key, String pointer) {

        if (index.containsKey(key)) {
            var pointers = index.get(key);

            pointers.add(pointer);

            index.put(key, pointers);

        } else {
            index.put(key, new ConcurrentSkipListSet<>(Collections.singleton(pointer)));
        }
    }

    public void removeFromIndex(ConcurrentSkipListMap<IndexKey, ConcurrentSkipListSet<String>> index, IndexKey key, String pointer) {

        var pointers = index.get(key);
        pointers.remove(pointer);

        if (pointers.isEmpty())
            index.remove(key);
        else
            index.put(key, pointers);
    }

    public String getCollectionPathFromPointer(String pointer) {
        return pointer.substring(0, pointer.indexOf("documents"));
    }

    public String pointerPathToIndexPath(String pointer, String field) {
        String collectionPath = getCollectionPathFromPointer(pointer);
        return Paths.get(collectionPath, "indexes", field + ".ser").toString();
    }

}