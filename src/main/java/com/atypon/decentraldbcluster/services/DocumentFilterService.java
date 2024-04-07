package com.atypon.decentraldbcluster.services;

import com.atypon.decentraldbcluster.disk.FileSystemService;
import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.utility.PathConstructor;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class DocumentFilterService {

    private final FileSystemService fileSystemService;
    private final DocumentIndexService documentIndexService;

    @Autowired
    public DocumentFilterService(FileSystemService fileSystemService, DocumentIndexService documentIndexService) {
        this.fileSystemService = fileSystemService;
        this.documentIndexService = documentIndexService;
    }


    public List<Document> filterDocuments(List<Document> documents, JsonNode filter) {

        List<Document> filteredDocuments = new ArrayList<>();

        for (var document: documents) {
            if (isDocumentMatch(filter, document.getContent()))
                filteredDocuments.add(document);
        }

        return filteredDocuments;
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


    public String getMostSelectiveIndexFiled(JsonNode filter, String collectionPath) throws Exception {
        List<String> indexedFields = documentIndexService.getIndexedFields(filter, collectionPath);
        if (indexedFields.isEmpty()) return null; // No indexes found

        int minSelectiveSize = Integer.MAX_VALUE;
        String mostSelectiveIndex = null;

        for (String field: indexedFields) {
            var index = fileSystemService.loadIndex( PathConstructor.constructUserGeneratedIndexPath(collectionPath, field) );

            var pointers = index.getPointers( filter.get(field) );

            if (pointers.size() < minSelectiveSize) {
                minSelectiveSize = pointers.size();
                mostSelectiveIndex = field;
            }

            if (minSelectiveSize == 0) return field;// Can't reduce any longer
        }
        return mostSelectiveIndex;
    }

}
