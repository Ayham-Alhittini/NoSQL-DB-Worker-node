package com.atypon.decentraldbcluster.api;

import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.services.*;
import com.atypon.decentraldbcluster.services.documenting.DocumentService;
import com.atypon.decentraldbcluster.services.documenting.DocumentVersionManager;
import com.atypon.decentraldbcluster.services.indexing.DocumentIndexService;
import com.atypon.decentraldbcluster.validation.DocumentValidator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/document")
public class DocumentController {

    private final UserDetails userDetails;
    private final DocumentService documentService;
    private final DocumentValidator documentValidator;
    private final DocumentVersionManager documentVersionManager;
    private final ObjectMapper mapper;
    private final DocumentIndexService documentIndexService;

    @Autowired
    public DocumentController(UserDetails userDetails, DocumentService documentService,
                              DocumentValidator documentValidator, ObjectMapper mapper,
                              DocumentVersionManager documentVersionManager, DocumentIndexService documentIndexService) {
        this.userDetails = userDetails;
        this.documentService = documentService;
        this.documentValidator = documentValidator;
        this.mapper = mapper;
        this.documentVersionManager = documentVersionManager;
        this.documentIndexService = documentIndexService;
    }

    //TODO: make validation on extra fields as well
    @PostMapping("{database}/{collection}/addDocument")
    public Document addDocument(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @RequestBody JsonNode documentData) throws Exception {
        Document document = new Document(documentData);

        String userDirectory = userDetails.getUserDirectory(request);
        String collectionPath = PathConstructor.constructCollectionPath(userDirectory, database, collection);
        String documentPath = PathConstructor.constructDocumentPath(collectionPath, document.getId());

        JsonNode schema = documentService.readSchema(collectionPath);

        documentValidator.doesDocumentMatchSchema(documentData, schema, true);

        FileStorageService.saveFile( mapper.valueToTree(document).toPrettyString() , documentPath);
        documentIndexService.insertToAllIndexes(document, documentPath);
        return document;
    }


    @DeleteMapping("{database}/{collection}/deleteDocument/{documentId}")
    public void deleteDocument(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @PathVariable String documentId) throws Exception {

        String userDirectory = userDetails.getUserDirectory(request);
        String collectionPath = PathConstructor.constructCollectionPath(userDirectory, database, collection);
        String documentPath = PathConstructor.constructDocumentPath(collectionPath, documentId);

        documentIndexService.deleteDocumentFromIndexes(documentPath);
        FileStorageService.deleteFile(documentPath);

    }

    // TODO:Eviction Strategy, for document version cashing
    @PatchMapping("{database}/{collection}/updateDocument/{documentId}")
    public Document updateDocument(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @PathVariable String documentId,@RequestParam int expectedVersion ,@RequestBody JsonNode requestBody) throws Exception {

        String userDirectory = userDetails.getUserDirectory(request);
        String collectionPath = PathConstructor.constructCollectionPath(userDirectory, database, collection);
        String documentPath = PathConstructor.constructDocumentPath(collectionPath, documentId);

        JsonNode schema = documentService.readSchema(collectionPath);

        documentValidator.doesDocumentMatchSchema(requestBody, schema, false);
        Document document = documentService.readDocument(documentPath);

        if (documentVersionManager.updateVersion(document, expectedVersion)) {

            JsonNode updatedDocumentData = documentService.patchDocument(requestBody, document.getData());
            document.setData(updatedDocumentData);

            documentIndexService.updateIndexes(document, requestBody, collectionPath);
            FileStorageService.saveFile( mapper.valueToTree(document).toPrettyString() , documentPath);

            return document;
        }

        throw new IllegalArgumentException("Conflict");
    }
}
