package com.atypon.decentraldbcluster.query.handlers.document;

import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.storage.managers.DocumentStorageManager;
import com.atypon.decentraldbcluster.query.types.DocumentQuery;
import com.atypon.decentraldbcluster.utility.PathConstructor;
import com.atypon.decentraldbcluster.schema.SchemaValidator;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ReplaceDocumentHandler {
    private final SchemaValidator schemaValidator;
    private final DocumentStorageManager documentStorageManager;
    private final AddDocumentHandler addDocumentHandler;
    private final DeleteDocumentHandler deleteDocumentHandler;

    @Autowired
    public ReplaceDocumentHandler(SchemaValidator schemaValidator,
                                  DocumentStorageManager documentPersistenceManager, AddDocumentHandler addDocumentHandler, DeleteDocumentHandler deleteDocumentHandler) {
        this.schemaValidator = schemaValidator;
        this.documentStorageManager = documentPersistenceManager;
        this.addDocumentHandler = addDocumentHandler;
        this.deleteDocumentHandler = deleteDocumentHandler;
    }

    public JsonNode handle(DocumentQuery query) throws Exception {

        String collectionPath = PathConstructor.constructCollectionPath(query);

        schemaValidator.validateDocument(query.getNewContent(), collectionPath, true);

        String documentPath = PathConstructor.constructDocumentPath(collectionPath, query.getDocumentId());
        deleteDocumentHandler.deleteDocumentFromIndexes(documentPath, query.getLoadedDocument());

        Document modifedDocument = getModifedDocument(query);
        addDocumentHandler.insertToAllDocumentIndexes(modifedDocument, documentPath);
        documentStorageManager.saveDocument(documentPath, modifedDocument);

        return modifedDocument.getContent();
    }



    private Document getModifedDocument(DocumentQuery query) {
        Document document = query.getLoadedDocument();
        document.setContent( document.appendIdToContent(query.getNewContent(), document.getId()) );
        document.incrementVersion();
        return document;
    }
}
