package com.atypon.decentraldbcluster.api.external;

import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.lock.OptimisticLocking;
import com.atypon.decentraldbcluster.query.QueryExecutor;
import com.atypon.decentraldbcluster.query.base.Query;
import com.atypon.decentraldbcluster.query.documents.DocumentQuery;
import com.atypon.decentraldbcluster.query.documents.DocumentQueryBuilder;
import com.atypon.decentraldbcluster.services.BroadcastService;
import com.atypon.decentraldbcluster.services.UserDetails;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/document")
public class DocumentController {

    private final UserDetails userDetails;
    private final QueryExecutor queryExecutor;
    private final BroadcastService broadcastService;
    private final OptimisticLocking optimisticLocking;

    @Autowired
    public DocumentController(UserDetails userDetails, OptimisticLocking optimisticLocking, QueryExecutor queryExecutor, BroadcastService broadcastService) {
        this.userDetails = userDetails;
        this.queryExecutor = queryExecutor;
        this.broadcastService = broadcastService;
        this.optimisticLocking = optimisticLocking;
    }

    @PostMapping("addDocument/{database}/{collection}")
    public Document addDocument(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @RequestBody JsonNode documentContent) throws Exception {

        DocumentQueryBuilder builder = new DocumentQueryBuilder();
        DocumentQuery query = builder
                .withOriginator(userDetails.getUserId(request))
                .withDatabase(database)
                .withCollection(collection)
                .addDocument(documentContent)
                .build();

        Document addedDocument = queryExecutor.exec(query, Document.class);
        query.setDocument(addedDocument);//To broadcast document with same ID and affinity port, as they generated dynamically

        broadcastService.doBroadcast(request, "document", query);
        return addedDocument;
    }


    @DeleteMapping("deleteDocument/{database}/{collection}/{documentId}")
    public void deleteDocument(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @PathVariable String documentId) throws Exception {

        Document document = (Document) request.getAttribute("document");

        DocumentQueryBuilder builder = new DocumentQueryBuilder();
        Query query = builder
                .withOriginator(userDetails.getUserId(request))
                .withDatabase(database)
                .withCollection(collection)
                .deleteDocument(document)
                .build();

        queryExecutor.exec(query);
        broadcastService.doBroadcast(request, "document", query);
    }


    @PatchMapping("updateDocument/{database}/{collection}/{documentId}")
    public Document updateDocument(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @PathVariable String documentId, @RequestParam int expectedVersion, @RequestBody JsonNode requestBody) throws Exception {
        return modifyDocument(request, database, collection, documentId, expectedVersion, requestBody, "UPDATE");
    }

    @PutMapping("replaceDocument/{database}/{collection}/{documentId}")
    public Document replaceDocument(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @PathVariable String documentId, @RequestParam int expectedVersion, @RequestBody JsonNode requestBody) throws Exception {
        return modifyDocument(request, database, collection, documentId, expectedVersion, requestBody, "REPLACE");
    }

    private Document modifyDocument(HttpServletRequest request, String database, String collection, String documentId, int expectedVersion, JsonNode newContent, String processType) throws Exception {
        Document document = (Document) request.getAttribute("document");

        if (optimisticLocking.attemptVersionUpdate(document, expectedVersion)) {

            try {
                DocumentQueryBuilder builder = new DocumentQueryBuilder();
                builder = builder
                        .withOriginator(userDetails.getUserId(request))
                        .withDatabase(database)
                        .withCollection(collection);

                builder = processType.equals("UPDATE") ? builder.updateDocument(document, newContent) : builder.replaceDocument(document, newContent);
                Query query = builder.build();

                Document updatedDocument = queryExecutor.exec(query, Document.class);
                broadcastService.doBroadcast(request, "document", query);
                return updatedDocument;
            } finally {
                optimisticLocking.clearDocumentVersion(documentId);// In case error happen.
            }
        }
        throw new IllegalArgumentException("Conflict");
    }
}
//TODO: make validation on extra fields as well for addDocument & Update document
//TODO:Eviction Strategy, for document version cashing