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

    private final ObjectMapper mapper;
    private final DocumentService documentService;

    @Autowired
    public QueryService(ObjectMapper mapper, DocumentService documentService) {
        this.mapper = mapper;
        this.documentService = documentService;
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

    public List<JsonNode> filterDocuments(List<JsonNode> documents, JsonNode filter) {

        List<JsonNode> filteredDocuments = new ArrayList<>();

        for (var document: documents) {
            boolean validDocument = true;
            validDocument = isDocumentMatch(filter, document, validDocument);

            if (validDocument)
                filteredDocuments.add(document);
        }

        return filteredDocuments;
    }

    private boolean isDocumentMatch(JsonNode filter, JsonNode document, boolean validDocument) {
        var iterator = filter.fields();

        while (iterator.hasNext()) {
            var field = iterator.next();

            if (document.get(field.getKey()) == null) {
                validDocument = false;
                break;
            }

            if (!document.get(field.getKey()).equals(field.getValue())) {
                validDocument = false;
                break;
            }
        }
        return validDocument;
    }


}
