package com.atypon.decentraldbcluster.query.documents.handlers;

import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.query.documents.DocumentQuery;
import com.atypon.decentraldbcluster.services.DocumentIndexService;
import com.atypon.decentraldbcluster.services.FileSystemService;
import com.atypon.decentraldbcluster.utility.PathConstructor;
import com.atypon.decentraldbcluster.validation.DocumentValidator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class ReplaceDocumentHandler {
    private final ObjectMapper mapper;
    private final DocumentValidator documentValidator;
    private final FileSystemService fileSystemService;
    private final DocumentIndexService documentIndexService;

    @Autowired
    public ReplaceDocumentHandler(ObjectMapper mapper, DocumentValidator documentValidator, FileSystemService fileSystemService, DocumentIndexService documentIndexService) {
        this.mapper = mapper;
        this.documentValidator = documentValidator;
        this.fileSystemService = fileSystemService;
        this.documentIndexService = documentIndexService;
    }

    public Document handle(DocumentQuery query) throws Exception {

        String collectionPath = PathConstructor.constructCollectionPath(query);

        documentValidator.validateDocument(query.getNewContent(), collectionPath, true);

        Document document = getModifedDocument(query);
        String documentPath = PathConstructor.constructDocumentPath(collectionPath, document.getId());

        replaceDocumentIndexes(document, documentPath);
        saveDocument(document, documentPath);

        return document;
    }

    private Document getModifedDocument(DocumentQuery query) {
        // Clone the document, because we need to broadcast the old document query
        Document document = new Document(query.getDocument());
        document.setContent(query.getNewContent());
        document.incrementVersion();
        return document;
    }

    private void replaceDocumentIndexes(Document document, String documentPath) throws Exception {
        documentIndexService.deleteDocumentFromIndexes(documentPath);
        documentIndexService.insertToAllDocumentIndexes(document, documentPath);
    }

    private void saveDocument(Document document, String documentPath) throws IOException {
        fileSystemService.saveFile( mapper.valueToTree(document).toPrettyString() , documentPath);
    }
}
