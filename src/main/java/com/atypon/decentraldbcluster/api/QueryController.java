package com.atypon.decentraldbcluster.api;

import com.atypon.decentraldbcluster.communication.affinity.dispatcher.DocumentAffinityDispatcher;
import com.atypon.decentraldbcluster.communication.braodcast.BroadcastType;
import com.atypon.decentraldbcluster.query.executors.QueryExecutor;
import com.atypon.decentraldbcluster.query.types.CollectionQuery;
import com.atypon.decentraldbcluster.query.types.DocumentQuery;
import com.atypon.decentraldbcluster.query.executors.DocumentQueryExecutor;
import com.atypon.decentraldbcluster.query.types.IndexQuery;
import com.atypon.decentraldbcluster.communication.braodcast.BroadcastService;
import com.atypon.decentraldbcluster.query.types.Query;
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
    private final DocumentAffinityDispatcher affinityDispatcher;


    @Autowired
    public QueryController(QueryExecutor queryExecutor, DocumentQueryExecutor documentQueryExecutor, BroadcastService broadcastService, DocumentAffinityDispatcher dispatcher) {
        this.queryExecutor = queryExecutor;
        this.broadcastService = broadcastService;
        this.documentQueryExecutor = documentQueryExecutor;
        this.affinityDispatcher = dispatcher;
    }

    @PostMapping("databaseQueries")
    public Object databaseQueries(@RequestBody DocumentQuery query) throws Exception {
        return executeQueryAndBroadcast(query, BroadcastType.DATABASE);
    }

    @PostMapping("collectionQueries")
    public Object collectionQueries(@RequestBody CollectionQuery query) throws Exception {
        return executeQueryAndBroadcast(query, BroadcastType.COLLECTION);
    }

    @PostMapping("indexQueries")
    public void indexQueries(@RequestBody IndexQuery query) throws Exception {
        executeQueryAndBroadcast(query, BroadcastType.INDEX);
    }

    @PostMapping("documentQueries")
    public Object documentQueries(HttpServletRequest request, @RequestBody DocumentQuery query) throws Exception {
        return executeAndBroadcastDocumentQuery(request, query);
    }

    private Object executeQueryAndBroadcast(Query query, BroadcastType type) throws Exception {
        var queryResult = queryExecutor.exec(query);
        broadcastService.doBroadcast(type, query);
        return queryResult;
    }

    private Object executeAndBroadcastDocumentQuery(HttpServletRequest request, DocumentQuery query) throws Exception {
        int affinityNodePort = affinityDispatcher.extractNodePortFromDocumentId(query);
        if (affinityDispatcher.shouldBeDispatchedToAffinity(query, affinityNodePort)) {
            return affinityDispatcher.dispatchToAffinity(request, query, affinityNodePort);
        }
        Object queryResult = documentQueryExecutor.execWithOptimisticLockingForModify(query);
        broadcastService.doBroadcast(BroadcastType.DOCUMENT, query);
        return queryResult;
    }
}
