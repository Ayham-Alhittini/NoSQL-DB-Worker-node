package com.atypon.decentraldbcluster.test.api;

import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.query.executors.QueryExecutor;
import com.atypon.decentraldbcluster.query.types.Query;
import com.atypon.decentraldbcluster.query.types.DocumentQuery;
import com.atypon.decentraldbcluster.test.builder.DocumentQueryBuilder;
import com.atypon.decentraldbcluster.query.executors.DocumentQueryExecutor;
import com.atypon.decentraldbcluster.secuirty.JwtService;
import com.atypon.decentraldbcluster.services.BroadcastService;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/document")
public class DocumentController {


    private final JwtService jwtService;
    private final QueryExecutor queryExecutor;
    private final BroadcastService broadcastService;
    private final DocumentQueryExecutor documentQueryExecutor;

    @Autowired
    public DocumentController(JwtService jwtService, QueryExecutor queryExecutor, BroadcastService broadcastService, DocumentQueryExecutor documentQueryExecutor) {
        this.jwtService = jwtService;
        this.queryExecutor = queryExecutor;
        this.broadcastService = broadcastService;
        this.documentQueryExecutor = documentQueryExecutor;
    }

    @PostMapping("addDocument/{database}/{collection}")
    public Document addDocument(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @RequestBody JsonNode documentContent) throws Exception {

        DocumentQueryBuilder builder = new DocumentQueryBuilder();
        DocumentQuery query = builder
                .withOriginator(jwtService.getUserId(request))
                .withDatabase(database)
                .withCollection(collection)
                .addDocument(documentContent)
                .build();

        Document addedDocument = queryExecutor.exec(query, Document.class);

        broadcastService.doBroadcast(request, "document", query);
        return addedDocument;
    }


    @DeleteMapping("deleteDocument/{database}/{collection}/{documentId}")
    public void deleteDocument(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @PathVariable String documentId) throws Exception {

        DocumentQueryBuilder builder = new DocumentQueryBuilder();
        Query query = builder
                .withOriginator(jwtService.getUserId(request))
                .withDatabase(database)
                .withCollection(collection)
                .deleteDocument(documentId)
                .build();

        queryExecutor.exec(query);
        broadcastService.doBroadcast(request, "document", query);
    }


    @PatchMapping("updateDocument/{database}/{collection}/{documentId}")
    public Document updateDocument(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @PathVariable String documentId, @RequestBody JsonNode newContent) throws Exception {

        DocumentQueryBuilder builder = new DocumentQueryBuilder();
        DocumentQuery query = builder
                .withOriginator(jwtService.getUserId(request))
                .withDatabase(database)
                .withCollection(collection)
                .updateDocument(documentId, newContent)
                .build();

        var result = (Document) documentQueryExecutor.execWithOptimisticLockingForModify(query);
        broadcastService.doBroadcast(request, "document", query);
        return result;
    }

    @PutMapping("replaceDocument/{database}/{collection}/{documentId}")
    public Document replaceDocument(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @PathVariable String documentId, @RequestBody JsonNode newContent) throws Exception {

        DocumentQueryBuilder builder = new DocumentQueryBuilder();
        DocumentQuery query = builder
                .withOriginator(jwtService.getUserId(request))
                .withDatabase(database)
                .withCollection(collection)
                .replaceDocument(documentId, newContent)
                .build();

        var result = (Document) documentQueryExecutor.execWithOptimisticLockingForModify(query);
        broadcastService.doBroadcast(request, "document", query);
        return result;
    }
}

//TODO: make validation on extra fields as well for addDocument & Update document