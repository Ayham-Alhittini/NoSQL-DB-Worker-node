package com.atypon.decentraldbcluster.query.handlers.document;

import com.atypon.decentraldbcluster.communication.affinity.AffinityLoadBalancer;
import com.atypon.decentraldbcluster.document.Document;
import com.atypon.decentraldbcluster.document.DocumentIndexService;
import com.atypon.decentraldbcluster.persistence.DocumentPersistenceManager;
import com.atypon.decentraldbcluster.query.types.DocumentQuery;
import com.atypon.decentraldbcluster.utility.PathConstructor;
import com.atypon.decentraldbcluster.validation.DocumentValidator;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Service;

@Service
public class AddDocumentHandler {

    private final DocumentValidator documentValidator;
    private final DocumentIndexService documentIndexService;
    private final AffinityLoadBalancer affinityLoadBalancer;
    private final DocumentPersistenceManager documentPersistenceManager;

    public AddDocumentHandler(DocumentValidator documentValidator, DocumentIndexService documentIndexService,
                              AffinityLoadBalancer affinityLoadBalancer, DocumentPersistenceManager documentPersistenceManager) {
        this.documentValidator = documentValidator;
        this.documentIndexService = documentIndexService;
        this.affinityLoadBalancer = affinityLoadBalancer;
        this.documentPersistenceManager = documentPersistenceManager;
    }

    public JsonNode handle(DocumentQuery query) throws Exception {

        String collectionPath = PathConstructor.constructCollectionPath(query);
        documentValidator.validateDocument(query.getContent(), collectionPath, true);

        Document document = createDocumentWithOptionalAssignedId(query);
        // To broadcast with same id
        query.setDocumentId(document.getId());

        String documentPath = PathConstructor.constructDocumentPath(collectionPath, document.getId());
        documentPersistenceManager.saveDocument(documentPath, document);
        documentIndexService.insertToAllDocumentIndexes(document, documentPath);

        return document.getContent();
    }


    // when we add a document we broadcast it, and to guaranty it have same ID we send
    // the document ID in the query
    private Document createDocumentWithOptionalAssignedId(DocumentQuery query) {
        if (query.getDocumentId() == null) {
            return new Document(query.getContent(), affinityLoadBalancer.getNextNodeNumber());
        } else {
            return new Document(query.getContent(), query.getDocumentId());
        }
    }
}
