package com.atypon.decentraldbcluster.api.external;

import com.atypon.decentraldbcluster.query.QueryExecutor;
import com.atypon.decentraldbcluster.query.base.Query;
import com.atypon.decentraldbcluster.query.collections.CollectionQueryBuilder;
import com.atypon.decentraldbcluster.services.BroadcastService;
import com.atypon.decentraldbcluster.services.UserDetails;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpMethod;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/collection")
@CrossOrigin("*")
public class CollectionController {

    private final UserDetails userDetails;
    private final QueryExecutor queryExecutor;

    @Autowired
    public CollectionController(UserDetails userDetails, QueryExecutor queryExecutor) {
        this.userDetails = userDetails;
        this.queryExecutor = queryExecutor;
    }

    @PostMapping("createCollection/{database}/{collection}")
    public void createCollection(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @RequestBody JsonNode schema) throws Exception {

        CollectionQueryBuilder builder = new CollectionQueryBuilder();

        Query query = builder
                .withOriginator(userDetails.getUserId(request))
                .withDatabase(database)
                .createCollection(collection)
                .withSchema(schema)
                .build();

        queryExecutor.exec(query);
        BroadcastService.doBroadcast(request, "createCollection/" + database + "/" + collection, schema, HttpMethod.POST);
    }

    @DeleteMapping("dropCollection/{database}/{collection}")
    public void deleteCollection(HttpServletRequest request, @PathVariable String database, @PathVariable String collection) throws Exception {

        CollectionQueryBuilder builder = new CollectionQueryBuilder();

        Query query = builder
                .withOriginator(userDetails.getUserId(request))
                .withDatabase(database)
                .dropCollection(collection)
                .build();

        queryExecutor.exec(query);
        BroadcastService.doBroadcast(request, "dropCollection/" + database + "/" + collection, null, HttpMethod.DELETE);
    }

    @GetMapping("showCollections/{database}")
    public Object showCollections(HttpServletRequest request, @PathVariable String database) throws Exception {

        CollectionQueryBuilder builder = new CollectionQueryBuilder();

        Query query = builder
                .withOriginator(userDetails.getUserId(request))
                .withDatabase(database)
                .showCollections()
                .build();

        return queryExecutor.exec(query);
    }
}




//TODO: create schema less collection