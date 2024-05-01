package com.atypon.decentraldbcluster.api;

import com.atypon.decentraldbcluster.query.executors.QueryExecutor;
import com.atypon.decentraldbcluster.query.types.CollectionQuery;
import com.atypon.decentraldbcluster.query.types.DatabaseQuery;
import com.atypon.decentraldbcluster.query.types.DocumentQuery;
import com.atypon.decentraldbcluster.query.types.IndexQuery;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/internal/api/broadcast")
public class BroadcastController {

    private final QueryExecutor queryExecutor;

    @Autowired
    public BroadcastController(QueryExecutor queryExecutor) {
        this.queryExecutor = queryExecutor;
    }

    @PostMapping("database")
    public void databaseBroadcast(@RequestBody DatabaseQuery query) throws Exception {
        queryExecutor.exec(query);
    }

    @PostMapping("collection")
    public void collectionBroadcast(@RequestBody CollectionQuery query) throws Exception {
        queryExecutor.exec(query);
    }

    @PostMapping("document")
    public void documentBroadcast(@RequestBody DocumentQuery query) throws Exception {
        queryExecutor.exec(query);
    }

    @PostMapping("index")
    public void indexBroadcast(@RequestBody IndexQuery query) throws Exception {
        queryExecutor.exec(query);
    }

}
