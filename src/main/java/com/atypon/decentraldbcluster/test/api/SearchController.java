package com.atypon.decentraldbcluster.test.api;

import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.query.executors.QueryExecutor;
import com.atypon.decentraldbcluster.query.types.Query;
import com.atypon.decentraldbcluster.test.builder.DocumentQueryBuilder;
import com.atypon.decentraldbcluster.secuirty.JwtService;
import com.atypon.decentraldbcluster.utility.ListCaster;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

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
    public Document findById(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @PathVariable String documentId) throws Exception {

        DocumentQueryBuilder builder = new DocumentQueryBuilder();

        Query query = builder
                .withOriginator( jwtService.getUserId(request) )
                .withDatabase(database)
                .withCollection(collection)
                .selectDocuments()
                .withId(documentId)
                .build();

        return queryExecutor.exec(query, Document.class);
    }


    @GetMapping("byFilter/{database}/{collection}")
    public List<Document> find(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @RequestBody JsonNode filter) throws Exception {

        DocumentQueryBuilder builder = new DocumentQueryBuilder();

        Query query = builder
                .withOriginator( jwtService.getUserId(request) )
                .withDatabase(database)
                .withCollection(collection)
                .selectDocuments()
                .withCondition(filter)
                .build();

        List<?> rawList = queryExecutor.exec(query, List.class);
        return ListCaster.castList(rawList, Document.class);
    }

}
