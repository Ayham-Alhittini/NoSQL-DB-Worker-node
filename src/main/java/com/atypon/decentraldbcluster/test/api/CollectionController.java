package com.atypon.decentraldbcluster.test.api;

import com.atypon.decentraldbcluster.query.executors.QueryExecutor;
import com.atypon.decentraldbcluster.query.types.Query;
import com.atypon.decentraldbcluster.test.builder.CollectionQueryBuilder;
import com.atypon.decentraldbcluster.secuirty.JwtService;
import com.atypon.decentraldbcluster.services.BroadcastService;
import com.atypon.decentraldbcluster.utility.ListCaster;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/collection")
@CrossOrigin("*")
public class CollectionController {

    private final JwtService jwtService;
    private final QueryExecutor queryExecutor;
    private final BroadcastService broadcastService;

    @Autowired
    public CollectionController(JwtService jwtService, QueryExecutor queryExecutor, BroadcastService broadcastService) {
        this.jwtService = jwtService;
        this.queryExecutor = queryExecutor;
        this.broadcastService = broadcastService;
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

        queryExecutor.exec(query);
        broadcastService.doBroadcast(request, "collection", query);
    }

    @DeleteMapping("dropCollection/{database}/{collection}")
    public void deleteCollection(HttpServletRequest request, @PathVariable String database, @PathVariable String collection) throws Exception {

        CollectionQueryBuilder builder = new CollectionQueryBuilder();

        Query query = builder
                .withOriginator(jwtService.getUserId(request))
                .withDatabase(database)
                .dropCollection(collection)
                .build();

        queryExecutor.exec(query);
        broadcastService.doBroadcast(request, "collection", query);
    }

    @GetMapping("showCollections/{database}")
    public List<String> showCollections(HttpServletRequest request, @PathVariable String database) throws Exception {

        CollectionQueryBuilder builder = new CollectionQueryBuilder();

        Query query = builder
                .withOriginator(jwtService.getUserId(request))
                .withDatabase(database)
                .showCollections()
                .build();

        List<?> rawList = queryExecutor.exec(query, List.class);
        return ListCaster.castList(rawList, String.class);
    }
}