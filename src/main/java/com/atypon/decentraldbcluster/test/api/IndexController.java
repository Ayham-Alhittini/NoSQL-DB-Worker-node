package com.atypon.decentraldbcluster.test.api;

import com.atypon.decentraldbcluster.query.executors.QueryExecutor;
import com.atypon.decentraldbcluster.query.types.Query;
import com.atypon.decentraldbcluster.test.builder.IndexQueryBuilder;
import com.atypon.decentraldbcluster.security.services.JwtService;
import com.atypon.decentraldbcluster.communication.braodcast.BroadcastService;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/index")
@CrossOrigin("*")
public class IndexController {


    private final JwtService jwtService;
    private final QueryExecutor queryExecutor;
    private final BroadcastService broadcastService;

    @Autowired
    public IndexController(JwtService jwtService, QueryExecutor queryExecutor, BroadcastService broadcastService) {
        this.jwtService = jwtService;
        this.queryExecutor = queryExecutor;
        this.broadcastService = broadcastService;
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

        queryExecutor.exec(query);
        broadcastService.doBroadcast("index", query);
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

        queryExecutor.exec(query);
        broadcastService.doBroadcast("index", query);
    }

}
