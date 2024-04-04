package com.atypon.decentraldbcluster.query.documents;

import com.atypon.decentraldbcluster.query.base.Executable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class DocumentQueryExecutor implements Executable<DocumentQuery> {
    private final DocumentHandler documentHandler;

    @Autowired
    public DocumentQueryExecutor(DocumentHandler documentHandler) {
        this.documentHandler = documentHandler;
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
}
