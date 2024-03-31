package com.atypon.decentraldbcluster.api.external;

import com.atypon.decentraldbcluster.affinity.AffinityLoadBalancer;
import com.atypon.decentraldbcluster.affinity.RedirectToAffinity;
import com.atypon.decentraldbcluster.config.NodeConfiguration;
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

    private final ObjectMapper mapper;
    private final UserDetails userDetails;
    private final DocumentService documentService;
    private final FileSystemService fileSystemService;
    private final OptimisticLocking optimisticLocking;
    private final DocumentValidator documentValidator;
    private final AffinityLoadBalancer affinityLoadBalancer;
    private final DocumentIndexService documentIndexService;

    @Autowired
    public DocumentController(UserDetails userDetails, DocumentService documentService,
                              DocumentValidator documentValidator, ObjectMapper mapper, FileSystemService fileSystemService,
                              DocumentIndexService documentIndexService, OptimisticLocking optimisticLocking, AffinityLoadBalancer affinityLoadBalancer) {
        this.mapper = mapper;
        this.userDetails = userDetails;
        this.documentService = documentService;
        this.documentValidator = documentValidator;
        this.fileSystemService = fileSystemService;
        this.optimisticLocking = optimisticLocking;
        this.documentIndexService = documentIndexService;
        this.affinityLoadBalancer = affinityLoadBalancer;
    }

    //TODO: make validation on extra fields as well
    @PostMapping("{database}/{collection}/addDocument")
    public Document addDocument(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @RequestBody JsonNode documentData) throws Exception {
        Document document = new Document(documentData, affinityLoadBalancer.getNextAffinityNodePort());

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

        Document document = documentService.readDocument(documentPath);
        //from here we should redirect if not affinity

        if (document.getAffinityPort() != NodeConfiguration.getCurrentNodePort()) {
            RedirectToAffinity.redirect(request, null, HttpMethod.DELETE, document.getAffinityPort());
            return;
        }

        affinityLoadBalancer.decrementNodeAssignedDocuments(document.getAffinityPort());

        documentIndexService.deleteDocumentFromIndexes(documentPath);
        fileSystemService.deleteFile(documentPath);

        BroadcastService.doBroadcast(request, "deleteDocument/" + database + "/" + collection + "/" + documentId, null, HttpMethod.DELETE);

    }

    // TODO:Eviction Strategy, for document version cashing
    @PutMapping("{database}/{collection}/updateDocument/{documentId}")
    public Object updateDocument(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @PathVariable String documentId,@RequestParam int expectedVersion ,@RequestBody JsonNode requestBody) throws Exception {

        String userDirectory = userDetails.getUserDirectory(request);
        String collectionPath = PathConstructor.constructCollectionPath(userDirectory, database, collection);
        String documentPath = PathConstructor.constructDocumentPath(collectionPath, documentId);

        Document document = documentService.readDocument(documentPath);
        //from here we should redirect if not affinity
        if (document.getAffinityPort() != NodeConfiguration.getCurrentNodePort()) {
            return RedirectToAffinity.redirect(request, requestBody, HttpMethod.PUT, document.getAffinityPort());
        }

        JsonNode schema = documentService.readSchema(collectionPath);
        documentValidator.doesDocumentMatchSchema(requestBody, schema, false);

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
