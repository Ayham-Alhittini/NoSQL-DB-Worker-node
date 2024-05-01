package com.atypon.decentraldbcluster.api.domain;

import com.atypon.decentraldbcluster.query.executors.QueryExecutor;
import com.atypon.decentraldbcluster.query.types.Query;
import com.atypon.decentraldbcluster.query.builder.DocumentQueryBuilder;
import com.atypon.decentraldbcluster.security.services.JwtService;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/search")
@CrossOrigin("*")
public class SearchController {

    private final JwtService jwtService;
    private final QueryExecutor queryExecutor;

    @Autowired
    public SearchController(JwtService jwtService, QueryExecutor queryExecutor) {
        this.jwtService = jwtService;
        this.queryExecutor = queryExecutor;
    }

    @GetMapping("byID/{database}/{collection}/{documentId}")
    public Object findById(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @PathVariable String documentId) throws Exception {

        DocumentQueryBuilder builder = new DocumentQueryBuilder();
        Query query = builder
                .withOriginator( jwtService.getUserId(request) )
                .withDatabase(database)
                .withCollection(collection)
                .selectDocuments()
                .withId(documentId)
                .build();

        return queryExecutor.exec(query);
    }


    @PostMapping("byFilter/{database}/{collection}")
    public Object find(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @RequestBody JsonNode filter) throws Exception {

        DocumentQueryBuilder builder = new DocumentQueryBuilder();
        Query query = builder
                .withOriginator( jwtService.getUserId(request) )
                .withDatabase(database)
                .withCollection(collection)
                .selectDocuments()
                .withCondition(filter)
                .build();

        return queryExecutor.exec(query);
    }

}
