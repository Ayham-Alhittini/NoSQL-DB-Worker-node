package com.atypon.decentraldbcluster.services;

import com.atypon.decentraldbcluster.entity.Document;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class QueryService {

    private final DocumentService documentService;
    private final IndexManager indexManager;
    private final DocumentIndexService documentIndexService;

    @Autowired
    public QueryService(DocumentService documentService, IndexManager indexManager, DocumentIndexService documentIndexService) {
        this.documentService = documentService;
        this.indexManager = indexManager;
        this.documentIndexService = documentIndexService;
    }

    public List<Document> filterDocuments(List<Document> documents, JsonNode filter) {

        List<Document> filteredDocuments = new ArrayList<>();

        for (var document: documents) {
            if (isDocumentMatch(filter, document.getData()))
                filteredDocuments.add(document);
        }

        return filteredDocuments;
    }

    public Document findDocumentById(String collectionPath, String documentId) throws Exception {

        String documentPath = PathConstructor.constructDocumentPath(collectionPath, documentId);
        return documentService.readDocument(documentPath);
    }


    public String getMostSelectiveIndexFiled(JsonNode filter, String collectionPath) throws Exception {
        List<String> indexedFields = documentIndexService.getIndexedFields(filter, collectionPath);
        int minSelectiveSize = Integer.MAX_VALUE;
        String mostSelectiveIndex = null;

        for (String field: indexedFields) {
            var index = indexManager.loadIndex( PathConstructor.constructUserGeneratedIndexPath(collectionPath, field) );

            var pointers = index.getPointers( filter.get(field) );

            if (pointers == null) continue;

            if (pointers.size() < minSelectiveSize) {
                minSelectiveSize = pointers.size();
                mostSelectiveIndex = field;
            }
        }
        return mostSelectiveIndex;
    }


    //TODO: you can further improve it by allowing sub objects.
    private boolean isDocumentMatch(JsonNode filter, JsonNode documentData) {
        var fields = filter.fields();

        while (fields.hasNext()) {
            var field = fields.next();

            if (documentData.get(field.getKey()) == null || !documentData.get(field.getKey()).equals(field.getValue()))
                return false;
        }
        return true;
    }

}
