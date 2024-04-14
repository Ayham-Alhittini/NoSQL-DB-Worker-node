package com.atypon.decentraldbcluster.query.handlers.document;

import com.atypon.decentraldbcluster.document.services.DocumentIndexService;
import com.atypon.decentraldbcluster.persistence.DocumentPersistenceManager;
import com.atypon.decentraldbcluster.query.types.DocumentQuery;
import com.atypon.decentraldbcluster.utility.PathConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DeleteDocumentHandler {
    private final DocumentIndexService documentIndexService;
    private final DocumentPersistenceManager documentPersistenceManager;
    @Autowired
    public DeleteDocumentHandler(DocumentIndexService documentIndexService, DocumentPersistenceManager documentPersistenceManager) {
        this.documentPersistenceManager = documentPersistenceManager;
        this.documentIndexService = documentIndexService;
    }

    public void handle(DocumentQuery query) throws Exception {

        String collectionPath = PathConstructor.constructCollectionPath(query);
        String documentPath = PathConstructor.constructDocumentPath(collectionPath, query.getDocumentId());

        documentIndexService.deleteDocumentFromIndexes(documentPath);
        documentPersistenceManager.removeDocument(documentPath);
    }

}
