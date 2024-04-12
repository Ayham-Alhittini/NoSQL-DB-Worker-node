package com.atypon.decentraldbcluster.query.handlers.document;

import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.query.types.DocumentQuery;
import com.atypon.decentraldbcluster.services.DocumentIndexService;
import com.atypon.decentraldbcluster.disk.FileSystemService;
import com.atypon.decentraldbcluster.services.DocumentReaderService;
import com.atypon.decentraldbcluster.utility.PathConstructor;
import com.atypon.decentraldbcluster.validation.DocumentValidator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class UpdateDocumentHandler {
    private final ObjectMapper mapper;
    private final DocumentValidator documentValidator;
    private final FileSystemService fileSystemService;
    private final DocumentIndexService documentIndexService;
    private final DocumentReaderService documentReaderService;

    @Autowired
    public UpdateDocumentHandler(ObjectMapper mapper, DocumentValidator documentValidator, FileSystemService fileSystemService,
                                 DocumentIndexService documentIndexService, DocumentReaderService documentReaderService) {
        this.mapper = mapper;
        this.documentValidator = documentValidator;
        this.fileSystemService = fileSystemService;
        this.documentIndexService = documentIndexService;
        this.documentReaderService = documentReaderService;
    }

    public JsonNode handle(DocumentQuery query) throws Exception {

        String collectionPath = PathConstructor.constructCollectionPath(query);

        documentValidator.validateDocument(query.getNewContent(), collectionPath, false);

        Document document = documentReaderService.findDocumentById(query);

        // Need to pass the old document, so updateIndexes track changes
        documentIndexService.updateIndexes(document, query.getNewContent(), collectionPath);
        updateDocument(document, query.getNewContent());
        saveDocument(document, collectionPath);

        return document.getContent();
    }


    private void updateDocument(Document document, JsonNode newContent) {
        JsonNode updatedDocumentData = integrateUpdate(document.getContent(), newContent);
        document.setContent(updatedDocumentData);
        document.incrementVersion();
    }

    private JsonNode integrateUpdate(JsonNode oldDocument, JsonNode requestBody) {
        ObjectNode newDocument = (ObjectNode) oldDocument;
        requestBody.fields().forEachRemaining(field -> newDocument.set(field.getKey(), field.getValue()));
        return newDocument;
    }

    private void saveDocument(Document document, String collectionPath) throws IOException {
        String documentPath = PathConstructor.constructDocumentPath(collectionPath, document.getId());
        fileSystemService.saveFile( mapper.valueToTree(document).toPrettyString() , documentPath);
    }
}
