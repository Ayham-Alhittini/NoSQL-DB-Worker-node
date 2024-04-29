package com.atypon.decentraldbcluster.query.executors;

import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.lock.OptimisticLocking;
import com.atypon.decentraldbcluster.query.actions.DocumentAction;
import com.atypon.decentraldbcluster.query.handlers.document.DocumentHandler;
import com.atypon.decentraldbcluster.query.types.DocumentQuery;
import com.atypon.decentraldbcluster.storage.managers.DocumentStorageManager;
import com.atypon.decentraldbcluster.utility.PathConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class DocumentQueryExecutor implements Executable<DocumentQuery> {
    private final DocumentHandler documentHandler;
    private final OptimisticLocking optimisticLocking;
    private final DocumentStorageManager documentStorageManager;


    @Autowired
    public DocumentQueryExecutor(DocumentHandler documentHandler, OptimisticLocking optimisticLocking,
                                 DocumentStorageManager documentStorageManager) {
        this.documentHandler = documentHandler;
        this.optimisticLocking = optimisticLocking;
        this.documentStorageManager = documentStorageManager;
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

        String documentPath = PathConstructor.constructDocumentPath(query);
        Document document = documentStorageManager.loadDocument(documentPath);

        if (optimisticLocking.attemptVersionUpdate(document, document.getVersion())) {
            try {
                return exec(query);
            } finally {
                optimisticLocking.releaseDocumentVersion(document.getId());
            }
        }
        throw new IllegalArgumentException("Document version conflict");
    }
}
