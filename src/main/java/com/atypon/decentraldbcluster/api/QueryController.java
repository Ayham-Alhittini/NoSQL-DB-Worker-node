package com.atypon.decentraldbcluster.api;

import com.atypon.decentraldbcluster.communication.affinity.DocumentAffinityDispatcher;
import com.atypon.decentraldbcluster.query.executors.QueryExecutor;
import com.atypon.decentraldbcluster.query.types.CollectionQuery;
import com.atypon.decentraldbcluster.query.types.DocumentQuery;
import com.atypon.decentraldbcluster.query.executors.DocumentQueryExecutor;
import com.atypon.decentraldbcluster.query.types.IndexQuery;
import com.atypon.decentraldbcluster.communication.braodcast.BroadcastService;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/query")
@CrossOrigin("*")
public class QueryController {

    private final QueryExecutor queryExecutor;
    private final BroadcastService broadcastService;
    private final DocumentAffinityDispatcher dispatcher;
    private final DocumentQueryExecutor documentQueryExecutor;

    @Autowired
    public QueryController(QueryExecutor queryExecutor, DocumentQueryExecutor documentQueryExecutor,
                           BroadcastService broadcastService, DocumentAffinityDispatcher dispatcher) {
        this.dispatcher = dispatcher;
        this.queryExecutor = queryExecutor;
        this.broadcastService = broadcastService;
        this.documentQueryExecutor = documentQueryExecutor;
    }

    @PostMapping("collectionQueries")
    public Object collectionQueries(HttpServletRequest request, @RequestBody CollectionQuery query) throws Exception {
        var queryResult = queryExecutor.exec(query);
        broadcastService.doBroadcast(request, "collection", query);
        return queryResult;
    }


    @PostMapping("documentQueries")
    public Object documentQueries(HttpServletRequest request, @RequestBody DocumentQuery query) throws Exception {
        if (dispatcher.shouldBeDispatchedToAffinity(query)) return dispatcher.dispatchToAffinity(request, query);
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
