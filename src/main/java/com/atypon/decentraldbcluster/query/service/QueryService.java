package com.atypon.decentraldbcluster.query.service;

import com.atypon.decentraldbcluster.communication.affinity.dispatcher.DocumentAffinityDispatcher;
import com.atypon.decentraldbcluster.communication.braodcast.BroadcastService;
import com.atypon.decentraldbcluster.communication.braodcast.BroadcastType;
import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.query.executors.DocumentQueryExecutor;
import com.atypon.decentraldbcluster.query.executors.QueryExecutor;
import com.atypon.decentraldbcluster.query.types.DocumentQuery;
import com.atypon.decentraldbcluster.query.types.Query;
import com.atypon.decentraldbcluster.storage.managers.DocumentStorageManager;
import com.atypon.decentraldbcluster.utility.PathConstructor;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class QueryService {

    private final QueryExecutor queryExecutor;
    private final BroadcastService broadcastService;
    private final DocumentQueryExecutor documentQueryExecutor;
    private final DocumentAffinityDispatcher affinityDispatcher;

    private final DocumentStorageManager documentStorageManager;

    @Autowired
    public QueryService(QueryExecutor queryExecutor, BroadcastService broadcastService, DocumentQueryExecutor documentQueryExecutor, DocumentAffinityDispatcher affinityDispatcher, DocumentStorageManager documentStorageManager) {
        this.queryExecutor = queryExecutor;
        this.broadcastService = broadcastService;
        this.documentQueryExecutor = documentQueryExecutor;
        this.affinityDispatcher = affinityDispatcher;
        this.documentStorageManager = documentStorageManager;
    }

    public Object executeQueryAndBroadcast(Query query, BroadcastType type) throws Exception {
        var queryResult = queryExecutor.exec(query);
        broadcastService.doBroadcastForWriteQuery(type, query);
        return queryResult;
    }

    public Object executeAndBroadcastDocumentQuery(HttpServletRequest request, DocumentQuery query) throws Exception {
        if (affinityDispatcher.isAffinityRelatedQuery(query)) {
            Document document = documentStorageManager.loadDocument( PathConstructor.constructDocumentPath(query) );
            query.setLoadedDocument(document);
            if (affinityDispatcher.shouldBeDispatchedToAffinity(document.getNodeAffinityPort())) {
                return affinityDispatcher.dispatchToAffinity(request, query, document.getNodeAffinityPort());
            }
            return executeDocumentQueryWithBroadcast(query);
        }
        return executeDocumentQueryWithBroadcast(query);
    }

    private Object executeDocumentQueryWithBroadcast(DocumentQuery query) throws Exception {
        Object queryResult = documentQueryExecutor.execWithOptimisticLockingForModify(query);
        broadcastService.doBroadcastForWriteQuery(BroadcastType.DOCUMENT, query);
        return queryResult;
    }
}
