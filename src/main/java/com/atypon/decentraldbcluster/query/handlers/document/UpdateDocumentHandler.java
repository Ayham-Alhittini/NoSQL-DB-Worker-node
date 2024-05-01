package com.atypon.decentraldbcluster.query.handlers.document;

import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.index.Index;
import com.atypon.decentraldbcluster.storage.managers.DocumentStorageManager;
import com.atypon.decentraldbcluster.query.types.DocumentQuery;
import com.atypon.decentraldbcluster.storage.managers.IndexStorageManager;
import com.atypon.decentraldbcluster.utility.IndexUtil;
import com.atypon.decentraldbcluster.utility.PathConstructor;
import com.atypon.decentraldbcluster.schema.SchemaValidator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class UpdateDocumentHandler {
    private final SchemaValidator schemaValidator;
    private final DocumentStorageManager documentStorageManager;
    private final IndexStorageManager indexStorageManager;

    @Autowired
    public UpdateDocumentHandler(SchemaValidator schemaValidator,
                                 DocumentStorageManager documentStorageManager, IndexStorageManager indexStorageManager) {
        this.schemaValidator = schemaValidator;
        this.documentStorageManager = documentStorageManager;
        this.indexStorageManager = indexStorageManager;
    }

    public JsonNode handle(DocumentQuery query) throws Exception {

        String collectionPath = PathConstructor.constructCollectionPath(query);

        schemaValidator.validateDocument(query.getNewContent(), collectionPath, false);

        String documentPath = PathConstructor.constructDocumentPath(collectionPath, query.getDocumentId());
        Document document = query.getLoadedDocument();

        // Need to pass the old document, so updateIndexes track changes
        updateDocumentIndexes(document, query.getNewContent(), collectionPath);
        updateDocument(document, query.getNewContent());
        documentStorageManager.saveDocument(documentPath, document);

        return document.getContent();
    }

    private void updateDocumentIndexes(Document document, JsonNode requestBody, String collectionPath) throws Exception {
        List<String> indexedFields = IndexUtil.getIndexedFields(requestBody, collectionPath);
        for (String field : indexedFields) {
            String indexPath = PathConstructor.constructIndexPath(collectionPath, field);
            Index index = indexStorageManager.loadIndex(indexPath);
            JsonNode oldKey = document.getContent().get(field);
            JsonNode newKey = requestBody.get(field);
            String documentPath = PathConstructor.constructDocumentPath(collectionPath, document.getId());

            if (!oldKey.equals(newKey)) { // Only update if the key has changed
                index.removePointer(oldKey, documentPath);
                index.addPointer(newKey, documentPath);
                indexStorageManager.saveIndex(indexPath, index);
            }
        }
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
