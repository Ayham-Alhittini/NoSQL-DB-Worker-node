package com.atypon.decentraldbcluster.api.external;

import com.atypon.decentraldbcluster.query.QueryExecutor;
import com.atypon.decentraldbcluster.query.collections.CollectionQuery;
import com.atypon.decentraldbcluster.query.documents.DocumentQuery;
import com.atypon.decentraldbcluster.query.documents.DocumentQueryExecutor;
import com.atypon.decentraldbcluster.query.index.IndexQuery;
import com.atypon.decentraldbcluster.services.BroadcastService;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/query")
@CrossOrigin("*")
public class QueryController {

    private final QueryExecutor queryExecutor;
    private final BroadcastService broadcastService;
    private final DocumentQueryExecutor documentQueryExecutor;

    @Autowired
    public QueryController(QueryExecutor queryExecutor, DocumentQueryExecutor documentQueryExecutor, BroadcastService broadcastService) {
        this.queryExecutor = queryExecutor;
        this.documentQueryExecutor = documentQueryExecutor;
        this.broadcastService = broadcastService;
    }

    @PostMapping("collectionQueries")
    public Object collectionQueries(HttpServletRequest request, @RequestBody CollectionQuery query) throws Exception {
        var queryResult = queryExecutor.exec(query);
        broadcastService.doBroadcast(request, "collection", query);
        return queryResult;
    }


    @PostMapping("documentQueries")
    public Object documentQueries(HttpServletRequest request, @RequestBody DocumentQuery query) throws Exception {
        var queryResult = documentQueryExecutor.execWithOptimisticLockingForModify(query);
        broadcastService.doBroadcast(request, "document", query);
        return queryResult;
    }

    @PostMapping("indexQueries")
    public void indexQueries(HttpServletRequest request, @RequestBody IndexQuery query) throws Exception {
        queryExecutor.exec(query);
        broadcastService.doBroadcast(request, "index", query);
    }
}
