package com.atypon.decentraldbcluster.query.handlers.document;

import com.atypon.decentraldbcluster.communication.affinity.balancer.AffinityLoadBalancer;
import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.storage.managers.DocumentStorageManager;
import com.atypon.decentraldbcluster.query.types.DocumentQuery;
import com.atypon.decentraldbcluster.storage.managers.IndexStorageManager;
import com.atypon.decentraldbcluster.utility.IndexUtil;
import com.atypon.decentraldbcluster.utility.PathConstructor;
import com.atypon.decentraldbcluster.schema.SchemaValidator;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class AddDocumentHandler {

    private final SchemaValidator schemaValidator;
    private final AffinityLoadBalancer affinityLoadBalancer;
    private final DocumentStorageManager documentStorageManager;
    private final IndexStorageManager indexStorageManager;

    public AddDocumentHandler(SchemaValidator schemaValidator, AffinityLoadBalancer affinityLoadBalancer,
                              DocumentStorageManager documentPersistenceManager, IndexStorageManager indexStorageManager) {
        this.schemaValidator = schemaValidator;
        this.affinityLoadBalancer = affinityLoadBalancer;
        this.documentStorageManager = documentPersistenceManager;
        this.indexStorageManager = indexStorageManager;
    }

    public JsonNode handle(DocumentQuery query) throws Exception {

        String collectionPath = PathConstructor.constructCollectionPath(query);
        schemaValidator.validateDocument(query.getContent(), collectionPath, true);

        Document document = createDocumentWithOptionalAssignedId(query);

        // Modify query to broadcast with same id
        query.setDocumentId(document.getId());
        query.setDocumentAffinityPort(document.getNodeAffinityPort());

        String documentPath = PathConstructor.constructDocumentPath(collectionPath, document.getId());
        documentStorageManager.saveDocument(documentPath, document);
        insertToAllDocumentIndexes(document, documentPath);

        return document.getContent();
    }

    public void insertToAllDocumentIndexes(Document document, String pointer) throws Exception {
        String collectionPath = PathConstructor.extractCollectionPathFromDocumentPath(pointer);
        List<String> indexedFields = IndexUtil.getIndexedFields(document.getContent(), collectionPath);

        for (String field : indexedFields) {
            String indexPath = PathConstructor.constructIndexPath(collectionPath, field);
            JsonNode key = document.getContent().get(field);
            indexStorageManager.addToIndex(indexPath, key, pointer);
        }
    }


    private Document createDocumentWithOptionalAssignedId(DocumentQuery query) {
        if (query.isBroadcastQuery()) {
            return createBroadcastDocument(query);
        } else {
            return createStandardDocument(query);
        }
    }

    private Document createBroadcastDocument(DocumentQuery query) {
        return new Document(query.getContent(), query.getDocumentId(), query.getDocumentAffinityPort());
    }

    private Document createStandardDocument(DocumentQuery query) {
        int affinityNodeOffset = affinityLoadBalancer.getNextAffinityNodeNumber();
        int port = calculatePort(affinityNodeOffset);

        if (hasCustomDocumentId(query)) {
            return new Document(query.getContent(), query.getDocumentId(), port);
        } else {
            return new Document(query.getContent(), port);
        }
    }

    private boolean hasCustomDocumentId(DocumentQuery query) {
        return query.getDocumentId() != null;
    }

    private int calculatePort(int affinityNodeOffset) {
        return 8080 + affinityNodeOffset;
    }
}
