package com.atypon.decentraldbcluster.api.domain;

import com.atypon.decentraldbcluster.communication.braodcast.BroadcastType;
import com.atypon.decentraldbcluster.query.service.QueryService;
import com.atypon.decentraldbcluster.query.types.Query;
import com.atypon.decentraldbcluster.query.builder.CollectionQueryBuilder;
import com.atypon.decentraldbcluster.security.services.JwtService;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/collection")
@CrossOrigin("*")
public class CollectionController {

    private final JwtService jwtService;
    private final QueryService queryService;

    @Autowired
    public CollectionController(JwtService jwtService, QueryService queryService) {
        this.jwtService = jwtService;
        this.queryService = queryService;
    }

    @PostMapping("createCollection/{database}/{collection}")
    public void createCollection(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @RequestBody(required = false) JsonNode schema) throws Exception {

        CollectionQueryBuilder builder = new CollectionQueryBuilder();
        Query query = builder
                .withOriginator(jwtService.getUserId(request))
                .withDatabase(database)
                .createCollection(collection)
                .withSchema(schema)
                .build();

        queryService.executeQueryAndBroadcast(query, BroadcastType.COLLECTION);
    }

    @DeleteMapping("dropCollection/{database}/{collection}")
    public void deleteCollection(HttpServletRequest request, @PathVariable String database, @PathVariable String collection) throws Exception {

        CollectionQueryBuilder builder = new CollectionQueryBuilder();
        Query query = builder
                .withOriginator(jwtService.getUserId(request))
                .withDatabase(database)
                .dropCollection(collection)
                .build();

        queryService.executeQueryAndBroadcast(query, BroadcastType.COLLECTION);
    }

    @GetMapping("getCollections/{database}")
    public Object showCollections(HttpServletRequest request, @PathVariable String database) throws Exception {

        CollectionQueryBuilder builder = new CollectionQueryBuilder();
        Query query = builder
                .withOriginator(jwtService.getUserId(request))
                .withDatabase(database)
                .showCollections()
                .build();

        return queryService.executeQueryAndBroadcast(query, BroadcastType.COLLECTION);
    }

    @GetMapping("getSchema/{database}/{collection}")
    public Object getSchema(HttpServletRequest request, @PathVariable String database, @PathVariable String collection) throws Exception {

        CollectionQueryBuilder builder = new CollectionQueryBuilder();
        Query query = builder
                .withOriginator(jwtService.getUserId(request))
                .withDatabase(database)
                .withCollection(collection)
                .showSchema()
                .build();

        return queryService.executeQueryAndBroadcast(query, BroadcastType.COLLECTION);
    }

}