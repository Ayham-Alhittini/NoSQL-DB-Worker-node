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

    public void createIndex(List<JsonNode> documents, String collectionPath, String field) throws Exception {

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

    public void serializeIndex(ConcurrentSkipListMap<IndexKey, ConcurrentSkipListSet<String>> index, String indexPath) {
        try (FileOutputStream fileOut = new FileOutputStream(indexPath);
             ObjectOutputStream out = new ObjectOutputStream(fileOut)) {
             out.writeObject(index);
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    public ConcurrentSkipListMap<IndexKey, ConcurrentSkipListSet<String>> deserializeIndex(String path) throws Exception {

        ConcurrentSkipListMap<IndexKey, ConcurrentSkipListSet<String>> index;

        try (FileInputStream fileIn = new FileInputStream(path);

             ObjectInputStream in = new ObjectInputStream(fileIn)) {
            index = (ConcurrentSkipListMap<IndexKey, ConcurrentSkipListSet<String>>) in.readObject();
        }
        return index;
    }


    private List<String> getIndexedFields(JsonNode jsonNode, String collectionPath) {
        List<String> indexedFields = new ArrayList<>();

        jsonNode.fields().forEachRemaining(field -> {
            String indexPath = constructIndexPath(collectionPath, field.getKey());
            if (Files.exists(Paths.get(indexPath))) {
                indexedFields.add(field.getKey());
            }
        });

        return indexedFields;
    }


    public String getMostSelectiveIndexFiled(JsonNode filter, String collectionPath) throws Exception {
        List<String> indexedFields = getIndexedFields(filter, collectionPath);
        int minSelectiveSize = Integer.MAX_VALUE;
        String mostSelectiveIndex = null;

        for (String field: indexedFields) {
            var index = deserializeIndex( constructIndexPath(collectionPath, field) );

            int indexSelectiveSize = index.get( new IndexKey(filter.get(field)) ).size();

            if (indexSelectiveSize < minSelectiveSize) {
                minSelectiveSize = indexSelectiveSize;
                mostSelectiveIndex = field;
            }
        }

        return mostSelectiveIndex;
    }


    public void insertToAllIndexes(JsonNode requestBody, String pointer) throws Exception {
        String collectionPath = getCollectionPathFromPointer(pointer);
        var indexedFields = getIndexedFields(requestBody, collectionPath);

        for (var field: indexedFields) {
            var fieldIndex = deserializeIndex( pointerPathToIndexPath(pointer, field) );
            addToIndex(fieldIndex, new IndexKey(requestBody.get(field)), pointer);
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

    public String getCollectionPathFromPointer(String pointer) {
        return pointer.substring(0, pointer.indexOf("documents"));
    }

    public String pointerPathToIndexPath(String pointer, String field) {
        String collectionPath = getCollectionPathFromPointer(pointer);
        return Paths.get(collectionPath, "indexes", field + ".ser").toString();
    }

}
