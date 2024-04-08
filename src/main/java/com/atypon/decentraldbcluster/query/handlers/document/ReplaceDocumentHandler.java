package com.atypon.decentraldbcluster.query.handlers.document;

import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.query.types.DocumentQuery;
import com.atypon.decentraldbcluster.services.DocumentIndexService;
import com.atypon.decentraldbcluster.disk.FileSystemService;
import com.atypon.decentraldbcluster.services.DocumentReaderService;
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
    private final DocumentReaderService documentReaderService;

    @Autowired
    public ReplaceDocumentHandler(ObjectMapper mapper, DocumentValidator documentValidator, FileSystemService fileSystemService,
                                  DocumentIndexService documentIndexService, DocumentReaderService documentReaderService) {
        this.mapper = mapper;
        this.documentValidator = documentValidator;
        this.fileSystemService = fileSystemService;
        this.documentIndexService = documentIndexService;
        this.documentReaderService = documentReaderService;
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

    private Document getModifedDocument(DocumentQuery query) throws Exception {
        Document document = documentReaderService.findDocumentById(query);
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
