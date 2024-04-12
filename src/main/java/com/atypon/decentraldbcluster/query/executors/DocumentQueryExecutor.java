package com.atypon.decentraldbcluster.query.executors;

import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.lock.OptimisticLocking;
import com.atypon.decentraldbcluster.query.actions.DocumentAction;
import com.atypon.decentraldbcluster.query.handlers.document.DocumentHandler;
import com.atypon.decentraldbcluster.query.types.DocumentQuery;
import com.atypon.decentraldbcluster.services.DocumentReaderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class DocumentQueryExecutor implements Executable<DocumentQuery> {
    private final DocumentHandler documentHandler;
    private final OptimisticLocking optimisticLocking;
    private final DocumentReaderService documentReaderService;

    @Autowired
    public DocumentQueryExecutor(DocumentHandler documentHandler, OptimisticLocking optimisticLocking,
                                 DocumentReaderService documentReaderService) {
        this.documentHandler = documentHandler;
        this.optimisticLocking = optimisticLocking;
        this.documentReaderService = documentReaderService;
    }

    @Override
    public Object exec(DocumentQuery query) throws Exception {
        return switch (query.getDocumentAction()) {
            case ADD -> documentHandler.handleAddDocument(query);
            case DELETE -> documentHandler.handleDeleteDocument(query);
            case UPDATE -> documentHandler.handleUpdateDocument(query);
            case REPLACE -> documentHandler.handleReplaceDocument(query);
            case SELECT -> documentHandler.handleSelectDocuments(query);
        };
    }

    public Object execWithOptimisticLockingForModify(DocumentQuery query) throws Exception {

        var action = query.getDocumentAction();
        if (action == DocumentAction.SELECT || action == DocumentAction.ADD)
            return exec(query);

        Document document = documentReaderService.findDocumentById(query);
        if (optimisticLocking.attemptVersionUpdate(document, document.getVersion())) {
            try {
                return exec(query);
            } finally {
                optimisticLocking.clearDocumentVersion(document.getId());
            }
        }
        throw new IllegalArgumentException("Document version conflict");
    }
}
