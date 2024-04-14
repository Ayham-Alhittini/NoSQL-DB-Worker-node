package com.atypon.decentraldbcluster.query.handlers.document;

import com.atypon.decentraldbcluster.document.entity.Document;
import com.atypon.decentraldbcluster.document.services.DocumentIndexService;
import com.atypon.decentraldbcluster.document.services.DocumentQueryService;
import com.atypon.decentraldbcluster.persistence.DocumentPersistenceManager;
import com.atypon.decentraldbcluster.query.types.DocumentQuery;
import com.atypon.decentraldbcluster.utility.PathConstructor;
import com.atypon.decentraldbcluster.validation.DocumentValidator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class UpdateDocumentHandler {
    private final DocumentValidator documentValidator;
    private final DocumentQueryService documentReaderService;
    private final DocumentIndexService documentIndexService;
    private final DocumentPersistenceManager documentPersistenceManager;

    @Autowired
    public UpdateDocumentHandler(DocumentValidator documentValidator, DocumentIndexService documentIndexService,
                                 DocumentQueryService documentReaderService, DocumentPersistenceManager documentPersistenceManager) {
        this.documentValidator = documentValidator;
        this.documentIndexService = documentIndexService;
        this.documentReaderService = documentReaderService;
        this.documentPersistenceManager = documentPersistenceManager;
    }

    public JsonNode handle(DocumentQuery query) throws Exception {

        String collectionPath = PathConstructor.constructCollectionPath(query);

        documentValidator.validateDocument(query.getNewContent(), collectionPath, false);

        Document document = documentReaderService.findDocumentById(query);
        String documentPath = PathConstructor.constructDocumentPath(collectionPath, document.getId());

        // Need to pass the old document, so updateIndexes track changes
        documentIndexService.updateIndexes(document, query.getNewContent(), collectionPath);
        updateDocument(document, query.getNewContent());
        documentPersistenceManager.saveDocument(documentPath, document);

        return document.getContent();
    }


    private void updateDocument(Document document, JsonNode newContent) {
        JsonNode updatedDocumentData = integrateUpdate(document.getContent(), newContent);
        document.setContent(updatedDocumentData);
        document.incrementVersion();
    }

    private JsonNode integrateUpdate(JsonNode oldDocument, JsonNode requestBody) {
        ObjectNode newDocument = (ObjectNode) oldDocument;
        requestBody.fields().forEachRemaining(field -> newDocument.set(field.getKey(), field.getValue()));
        return newDocument;
    }
}
