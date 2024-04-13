package com.atypon.decentraldbcluster.query.handlers.document;

import com.atypon.decentraldbcluster.document.Document;
import com.atypon.decentraldbcluster.document.DocumentIndexService;
import com.atypon.decentraldbcluster.document.DocumentQueryService;
import com.atypon.decentraldbcluster.persistence.DocumentPersistenceManager;
import com.atypon.decentraldbcluster.query.types.DocumentQuery;
import com.atypon.decentraldbcluster.utility.PathConstructor;
import com.atypon.decentraldbcluster.validation.DocumentValidator;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ReplaceDocumentHandler {
    private final DocumentValidator documentValidator;
    private final DocumentQueryService documentReaderService;
    private final DocumentIndexService documentIndexService;
    private final DocumentPersistenceManager documentPersistenceManager;

    @Autowired
    public ReplaceDocumentHandler(DocumentValidator documentValidator, DocumentIndexService documentIndexService,
                                  DocumentQueryService documentReaderService, DocumentPersistenceManager documentPersistenceManager) {
        this.documentValidator = documentValidator;
        this.documentIndexService = documentIndexService;
        this.documentReaderService = documentReaderService;
        this.documentPersistenceManager = documentPersistenceManager;
    }

    public JsonNode handle(DocumentQuery query) throws Exception {

        String collectionPath = PathConstructor.constructCollectionPath(query);

        documentValidator.validateDocument(query.getNewContent(), collectionPath, true);

        Document document = getModifedDocument(query);
        String documentPath = PathConstructor.constructDocumentPath(collectionPath, document.getId());

        replaceDocumentIndexes(document, documentPath);
        documentPersistenceManager.saveDocument(documentPath, document);

        return document.getContent();
    }

    private Document getModifedDocument(DocumentQuery query) throws Exception {
        Document document = documentReaderService.findDocumentById(query);
        document.setContent( document.appendIdToContent(query.getNewContent(), document.getId()) );
        document.incrementVersion();
        return document;
    }

    private void replaceDocumentIndexes(Document document, String documentPath) throws Exception {
        documentIndexService.deleteDocumentFromIndexes(documentPath);
        documentIndexService.insertToAllDocumentIndexes(document, documentPath);
    }
}