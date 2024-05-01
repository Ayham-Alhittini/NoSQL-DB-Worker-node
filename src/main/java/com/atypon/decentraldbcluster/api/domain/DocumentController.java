package com.atypon.decentraldbcluster.api.domain;

import com.atypon.decentraldbcluster.query.service.QueryService;
import com.atypon.decentraldbcluster.query.types.DocumentQuery;
import com.atypon.decentraldbcluster.query.builder.DocumentQueryBuilder;
import com.atypon.decentraldbcluster.security.services.JwtService;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/document")
@CrossOrigin("*")
public class DocumentController {


    private final JwtService jwtService;
    private final QueryService queryService;

    @Autowired
    public DocumentController(JwtService jwtService, QueryService queryService) {
        this.jwtService = jwtService;
        this.queryService = queryService;
    }

    @PostMapping("addDocument/{database}/{collection}")
    public Object addDocument(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @RequestBody JsonNode documentContent) throws Exception {

        DocumentQueryBuilder builder = new DocumentQueryBuilder();
        DocumentQuery query = builder
                .withOriginator(jwtService.getUserId(request))
                .withDatabase(database)
                .withCollection(collection)
                .addDocument(documentContent)
                .build();

        return queryService.executeAndBroadcastDocumentQuery(request, query);
    }


    @DeleteMapping("deleteDocument/{database}/{collection}/{documentId}")
    public Object deleteDocument(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @PathVariable String documentId) throws Exception {

        DocumentQueryBuilder builder = new DocumentQueryBuilder();
        DocumentQuery query = builder
                .withOriginator(jwtService.getUserId(request))
                .withDatabase(database)
                .withCollection(collection)
                .deleteDocument(documentId)
                .build();

        return queryService.executeAndBroadcastDocumentQuery(request, query);
    }


    @PatchMapping("updateDocument/{database}/{collection}/{documentId}")
    public Object updateDocument(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @PathVariable String documentId, @RequestBody JsonNode newContent) throws Exception {

        DocumentQueryBuilder builder = new DocumentQueryBuilder();
        DocumentQuery query = builder
                .withOriginator(jwtService.getUserId(request))
                .withDatabase(database)
                .withCollection(collection)
                .updateDocument(documentId, newContent)
                .build();

        return queryService.executeAndBroadcastDocumentQuery(request, query);
    }

    @PutMapping("replaceDocument/{database}/{collection}/{documentId}")
    public Object replaceDocument(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @PathVariable String documentId, @RequestBody JsonNode newContent) throws Exception {

        DocumentQueryBuilder builder = new DocumentQueryBuilder();
        DocumentQuery query = builder
                .withOriginator(jwtService.getUserId(request))
                .withDatabase(database)
                .withCollection(collection)
                .replaceDocument(documentId, newContent)
                .build();

        return queryService.executeAndBroadcastDocumentQuery(request, query);
    }
}
