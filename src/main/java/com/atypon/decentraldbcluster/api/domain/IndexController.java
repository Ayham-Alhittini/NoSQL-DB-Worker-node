package com.atypon.decentraldbcluster.api.domain;

import com.atypon.decentraldbcluster.communication.braodcast.QueryBroadcastType;
import com.atypon.decentraldbcluster.query.service.QueryService;
import com.atypon.decentraldbcluster.query.types.Query;
import com.atypon.decentraldbcluster.query.builder.IndexQueryBuilder;
import com.atypon.decentraldbcluster.security.services.JwtService;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/index")
@CrossOrigin("*")
public class IndexController {


    private final JwtService jwtService;
    private final QueryService queryService;

    @Autowired
    public IndexController(JwtService jwtService, QueryService queryService) {
        this.jwtService = jwtService;
        this.queryService = queryService;
    }

    @PostMapping("createIndex/{database}/{collection}/{field}")
    public void createIndex(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @PathVariable String field) throws Exception {

        IndexQueryBuilder builder = new IndexQueryBuilder();
        Query query = builder
                .withOriginator( jwtService.getUserId(request) )
                .withDatabase(database)
                .withCollection(collection)
                .createIndex(field)
                .build();

        queryService.executeQueryAndBroadcast(query, QueryBroadcastType.INDEX);
    }

    @DeleteMapping("dropIndex/{database}/{collection}/{field}")
    public void deleteIndex(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @PathVariable String field) throws Exception {

        IndexQueryBuilder builder = new IndexQueryBuilder();
        Query query = builder
                .withOriginator( jwtService.getUserId(request) )
                .withDatabase(database)
                .withCollection(collection)
                .dropIndex(field)
                .build();

        queryService.executeQueryAndBroadcast(query, QueryBroadcastType.INDEX);
    }

    @GetMapping("getIndexes/{database}/{collection}")
    public Object getIndexes(HttpServletRequest request, @PathVariable String database, @PathVariable String collection) throws Exception {

        IndexQueryBuilder builder = new IndexQueryBuilder();
        Query query = builder
                .withOriginator( jwtService.getUserId(request) )
                .withDatabase(database)
                .withCollection(collection)
                .showIndexes()
                .build();

        return queryService.executeQueryAndBroadcast(query, QueryBroadcastType.INDEX);
    }

}
