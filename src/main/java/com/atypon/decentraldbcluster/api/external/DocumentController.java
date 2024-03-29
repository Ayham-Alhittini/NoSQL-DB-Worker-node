package com.atypon.decentraldbcluster.api.external;

import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.lock.OptimisticLocking;
import com.atypon.decentraldbcluster.services.*;
import com.atypon.decentraldbcluster.validation.DocumentValidator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpMethod;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/document")
public class DocumentController {

    private final UserDetails userDetails;
    private final DocumentService documentService;
    private final DocumentValidator documentValidator;
    private final ObjectMapper mapper;
    private final FileSystemService fileSystemService;
    private final DocumentIndexService documentIndexService;
    private final OptimisticLocking optimisticLocking;

    @Autowired
    public DocumentController(UserDetails userDetails, DocumentService documentService,
                              DocumentValidator documentValidator, ObjectMapper mapper, FileSystemService fileSystemService,
                              DocumentIndexService documentIndexService, OptimisticLocking optimisticLocking) {
        this.userDetails = userDetails;
        this.documentService = documentService;
        this.documentValidator = documentValidator;
        this.mapper = mapper;
        this.fileSystemService = fileSystemService;
        this.documentIndexService = documentIndexService;
        this.optimisticLocking = optimisticLocking;
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

        fileSystemService.saveFile( mapper.valueToTree(document).toPrettyString() , documentPath);
        documentIndexService.insertToAllIndexes(document, documentPath);

        BroadcastService.doBroadcast(request, "addDocument/" + database + "/" + collection, mapper.valueToTree(document), HttpMethod.POST);

        return document;
    }


    @DeleteMapping("{database}/{collection}/deleteDocument/{documentId}")
    public void deleteDocument(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @PathVariable String documentId) throws Exception {

        String userDirectory = userDetails.getUserDirectory(request);
        String collectionPath = PathConstructor.constructCollectionPath(userDirectory, database, collection);
        String documentPath = PathConstructor.constructDocumentPath(collectionPath, documentId);

        documentIndexService.deleteDocumentFromIndexes(documentPath);
        fileSystemService.deleteFile(documentPath);

        BroadcastService.doBroadcast(request, "deleteDocument/" + database + "/" + collection + "/" + documentId, null, HttpMethod.DELETE);

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

        if (optimisticLocking.attemptVersionUpdate(document, expectedVersion)) {
            try {
                JsonNode updatedDocumentData = documentService.patchDocument(requestBody, document.getData());
                document.setData(updatedDocumentData);
                document.incrementVersion();

                documentIndexService.updateIndexes(document, requestBody, collectionPath);
                fileSystemService.saveFile( mapper.valueToTree(document).toPrettyString() , documentPath);

                BroadcastService.doBroadcast(request, "updateDocument/" + database + "/" + collection + "/" + documentId, requestBody, HttpMethod.PUT);

            } finally {
                optimisticLocking.clearDocumentVersion(documentId);// To prevent storing unneeded document
            }
            return document;
        }

        throw new IllegalArgumentException("Conflict");
    }
}
