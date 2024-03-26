package com.atypon.decentraldbcluster.api;

import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.services.DocumentService;
import com.atypon.decentraldbcluster.services.FileStorageService;
import com.atypon.decentraldbcluster.services.IndexService;
import com.atypon.decentraldbcluster.services.UserDetails;
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
    private final ObjectMapper mapper;
    private final IndexService indexService;

    @Autowired
    public DocumentController(UserDetails userDetails, DocumentService documentService, DocumentValidator documentValidator, ObjectMapper mapper, IndexService indexService) {
        this.userDetails = userDetails;
        this.documentService = documentService;
        this.documentValidator = documentValidator;
        this.mapper = mapper;
        this.indexService = indexService;
    }

    @PostMapping("{database}/{collection}/addDocument")
    public Document addDocument(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @RequestBody JsonNode documentData) throws Exception {
        //TODO: make validation on extra fields as well
        Document document = new Document(documentData);

        String userDirectory = userDetails.getUserDirectory(request);
        String collectionPath = FileStorageService.constructCollectionPath(userDirectory, database, collection);
        String documentPath = documentService.constructDocumentPath(collectionPath, document.getId());

        JsonNode schema = documentService.readSchema(collectionPath);

        documentValidator.doesDocumentMatchSchema(documentData, schema, true);

        FileStorageService.saveFile( mapper.valueToTree(document).toPrettyString() , documentPath);
        indexService.insertToAllIndexes(document, documentPath);
        return document;
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
    public Document updateDocument(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @PathVariable String documentId, @RequestBody JsonNode requestBody) throws Exception {

        String userDirectory = userDetails.getUserDirectory(request);
        String collectionPath = FileStorageService.constructCollectionPath(userDirectory, database, collection);
        String documentPath = documentService.constructDocumentPath(collectionPath, documentId);

        JsonNode schema = documentService.readSchema(collectionPath);

        documentValidator.doesDocumentMatchSchema(requestBody, schema, false);

        Document document = documentService.readDocument(documentPath);
        indexService.updateIndexes(document, requestBody, collectionPath);

        JsonNode updatedDocumentData = documentService.updateDocument(requestBody, document.getData());
        document.setData(updatedDocumentData);

        FileStorageService.saveFile( mapper.valueToTree(document).toPrettyString() , documentPath);

        return document;
    }
    // TODO:Eviction Strategy, for document version cashing
}
