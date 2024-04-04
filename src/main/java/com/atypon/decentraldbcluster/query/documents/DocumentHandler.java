package com.atypon.decentraldbcluster.query.documents;

import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.query.documents.handlers.*;
import org.springframework.stereotype.Service;

@Service
public class DocumentHandler {
    private final AddDocumentHandler addDocumentHandler;
    private final DeleteDocumentHandler deleteDocumentHandler;
    private final UpdateDocumentHandler updateDocumentHandler;
    private final SelectDocumentsHandler selectDocumentsHandler;
    private final ReplaceDocumentHandler replaceDocumentHandler;

    public DocumentHandler(DeleteDocumentHandler deleteDocumentHandler, AddDocumentHandler addDocumentHandler, UpdateDocumentHandler updateDocumentHandler,
                           SelectDocumentsHandler selectDocumentsHandler, ReplaceDocumentHandler replaceDocumentHandler) {
        this.deleteDocumentHandler = deleteDocumentHandler;
        this.addDocumentHandler = addDocumentHandler;
        this.updateDocumentHandler = updateDocumentHandler;
        this.selectDocumentsHandler = selectDocumentsHandler;
        this.replaceDocumentHandler = replaceDocumentHandler;
    }

    public Document handleAddDocument(DocumentQuery query) throws Exception {
        return addDocumentHandler.handle(query);
    }

    public Void handleDeleteDocument(DocumentQuery query) throws Exception {
        deleteDocumentHandler.handle(query);
        return null;
    }

    public Document handleUpdateDocument(DocumentQuery query) throws Exception {
        return updateDocumentHandler.handle(query);
    }

    public Document handleReplaceDocument(DocumentQuery query) throws Exception {
        return replaceDocumentHandler.handle(query);
    }

    public Object handleSelectDocuments(DocumentQuery query) throws Exception {
        return selectDocumentsHandler.handle(query);
    }
}
