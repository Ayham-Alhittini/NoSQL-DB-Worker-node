package com.atypon.decentraldbcluster.services;

import com.atypon.decentraldbcluster.entity.IndexKey;
import com.atypon.decentraldbcluster.error.ResourceNotFoundException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

@Service
public class QueryService {

    private final DocumentService documentService;
    private final IndexService indexService;
    private final ObjectMapper mapper;

    @Autowired
    public QueryService(DocumentService documentService, IndexService indexService, ObjectMapper mapper) {
        this.documentService = documentService;
        this.indexService = indexService;
        this.mapper = mapper;
    }

    public List<JsonNode> filterDocuments(List<JsonNode> documents, JsonNode filter) {

        List<JsonNode> filteredDocuments = new ArrayList<>();

        for (var document: documents) {
            if (isDocumentMatch(filter, document))
                filteredDocuments.add(document);
        }

        return filteredDocuments;
    }

    public JsonNode findDocumentById(ConcurrentSkipListMap<IndexKey, ConcurrentSkipListSet<String>> index, String documentId) throws IOException {
        JsonNode node = mapper.readTree('\"' + documentId + '\"');
        IndexKey key = new IndexKey(node);
        if (index.containsKey(key)) {
            String pointer = index.get(key).first();
            return documentService.readDocument(pointer);
        }
        throw new ResourceNotFoundException("Document not exists");
    }


    public String getMostSelectiveIndexFiled(JsonNode filter, String collectionPath) throws Exception {
        List<String> indexedFields = indexService.getIndexedFields(filter, collectionPath);
        int minSelectiveSize = Integer.MAX_VALUE;
        String mostSelectiveIndex = null;

        for (String field: indexedFields) {
            var index = indexService.deserializeIndex( indexService.constructIndexPath(collectionPath, field) );

            var pointers = indexService.getPointers(index, filter.get(field) );

            if (pointers == null) continue;

            if (pointers.size() < minSelectiveSize) {
                minSelectiveSize = pointers.size();
                mostSelectiveIndex = field;
            }
        }
        return mostSelectiveIndex;
    }


    private boolean isDocumentMatch(JsonNode filter, JsonNode document) {
        var fields = filter.fields();

        while (fields.hasNext()) {
            var field = fields.next();

            if (document.get(field.getKey()) == null || !document.get(field.getKey()).equals(field.getValue()))
                return false;
        }
        return true;
    }

}
