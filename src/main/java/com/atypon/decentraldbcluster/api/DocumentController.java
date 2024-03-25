package com.atypon.decentraldbcluster.api;

import com.atypon.decentraldbcluster.index.ObjectId;
import com.atypon.decentraldbcluster.schema.SchemaValidator;
import com.atypon.decentraldbcluster.services.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/document")
public class DocumentController {

    private final UserDetails userDetails;
    private final DocumentService documentService;
    private final SchemaValidator schemaValidator;
    private final ObjectMapper mapper;
    private final IndexService indexService;

    @Autowired
    public DocumentController(UserDetails userDetails, DocumentService documentService, SchemaValidator schemaValidator, ObjectMapper mapper, IndexService indexService) {
        this.userDetails = userDetails;
        this.documentService = documentService;
        this.schemaValidator = schemaValidator;
        this.mapper = mapper;
        this.indexService = indexService;
    }

    @PostMapping("{database}/{collection}/addDocument")
    public void addDocument(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @RequestBody Map<String, Object> requestBody) throws Exception {

        ObjectId objectId = documentService.createAndAppendDocumentId(requestBody);

        String userDirectory = userDetails.getUserDirectory(request);
        String collectionPath = FileStorageService.constructCollectionPath(userDirectory, database, collection);
        String documentPath = documentService.constructDocumentPath(collectionPath, objectId.toHexString());

        JsonNode schema = documentService.readSchema(collectionPath);
        JsonNode document = mapper.valueToTree(requestBody);

        schemaValidator.doesDocumentMatchSchema(document, schema, true);

        FileStorageService.saveFile(document.toPrettyString(), documentPath);
        indexService.insertToAllIndexes(document, documentPath);

    }


    @DeleteMapping("{database}/{collection}/deleteDocument/{documentId}")
    public void deleteDocument(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @PathVariable String documentId) throws Exception {

        String userDirectory = userDetails.getUserDirectory(request);
        String collectionPath = FileStorageService.constructCollectionPath(userDirectory, database, collection);
        String documentPath = documentService.constructDocumentPath(collectionPath, documentId);

        indexService.deleteDocumentFromIndexes(documentPath);
        FileStorageService.deleteFile(documentPath);

    }

    @PatchMapping("{database}/{collection}/updateDocument/{documentId}")
    public JsonNode updateDocument(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @PathVariable String documentId, @RequestBody JsonNode requestBody) throws Exception {

        String userDirectory = userDetails.getUserDirectory(request);
        String collectionPath = FileStorageService.constructCollectionPath(userDirectory, database, collection);
        String documentPath = documentService.constructDocumentPath(collectionPath, documentId);

        JsonNode currentDocument = documentService.readDocument(documentPath);
        JsonNode schema = documentService.readSchema(collectionPath);

        schemaValidator.doesDocumentMatchSchema(requestBody, schema, false);

        indexService.updateIndexes(currentDocument, requestBody, collectionPath);

        JsonNode updatedDocument = documentService.updateDocument(requestBody, currentDocument);
        FileStorageService.saveFile(updatedDocument.toPrettyString(), documentPath);

        return updatedDocument;
    }

}
