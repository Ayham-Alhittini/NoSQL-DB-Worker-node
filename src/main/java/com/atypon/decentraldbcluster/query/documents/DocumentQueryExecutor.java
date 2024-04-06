package com.atypon.decentraldbcluster.query.documents;

import com.atypon.decentraldbcluster.lock.OptimisticLocking;
import com.atypon.decentraldbcluster.query.base.Executable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

@Component
public class DocumentQueryExecutor implements Executable<DocumentQuery> {
    private final DocumentHandler documentHandler;
    private final OptimisticLocking optimisticLocking;

    @Autowired
    public DocumentQueryExecutor(DocumentHandler documentHandler, OptimisticLocking optimisticLocking) {
        this.documentHandler = documentHandler;
        this.optimisticLocking = optimisticLocking;
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
        if (action != DocumentAction.UPDATE && action != DocumentAction.REPLACE)
            return exec(query);

        var document = query.getDocument();
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
