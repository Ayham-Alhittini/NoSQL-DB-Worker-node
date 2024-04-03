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
    private final OptimisticLocking optimisticLocking;

    @Autowired
    public DocumentController(UserDetails userDetails, OptimisticLocking optimisticLocking, QueryExecutor queryExecutor) {
        this.userDetails = userDetails;
        this.queryExecutor = queryExecutor;
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

        query.setDocument(addedDocument);
        BroadcastService.doBroadcast(request, "document", query);
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
        BroadcastService.doBroadcast(request, "document", query);
    }


    @PutMapping("updateDocument/{database}/{collection}/{documentId}")
    public Document updateDocument(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @PathVariable String documentId, @RequestParam int expectedVersion, @RequestBody JsonNode requestBody) throws Exception {

        Document document = (Document) request.getAttribute("document");

        if (optimisticLocking.attemptVersionUpdate(document, expectedVersion)) {

            DocumentQueryBuilder builder = new DocumentQueryBuilder();
            Query query = builder
                    .withOriginator(userDetails.getUserId(request))
                    .withDatabase(database)
                    .withCollection(collection)
                    .updateDocument(document, requestBody)
                    .build();

            Document updatedDocument = queryExecutor.exec(query, Document.class);
            optimisticLocking.clearDocumentVersion(documentId);// To prevent storing unneeded document

            BroadcastService.doBroadcast(request, "document", query);
            return updatedDocument;
        }
        throw new IllegalArgumentException("Conflict");
    }
}




//TODO: make validation on extra fields as well for addDocument
//TODO:Eviction Strategy, for document version cashing