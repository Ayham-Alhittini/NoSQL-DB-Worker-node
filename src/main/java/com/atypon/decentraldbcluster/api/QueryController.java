package com.atypon.decentraldbcluster.api;

import com.atypon.decentraldbcluster.communication.braodcast.BroadcastType;
import com.atypon.decentraldbcluster.query.service.QueryService;
import com.atypon.decentraldbcluster.query.types.CollectionQuery;
import com.atypon.decentraldbcluster.query.types.DocumentQuery;
import com.atypon.decentraldbcluster.query.types.IndexQuery;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/query")
@CrossOrigin("*")
public class QueryController {

    private final QueryService queryService;

    @Autowired
    public QueryController(QueryService queryService) {
        this.queryService = queryService;
    }


    @PostMapping("databaseQueries")
    public Object databaseQueries(@RequestBody DocumentQuery query) throws Exception {
        return queryService.executeQueryAndBroadcast(query, BroadcastType.DATABASE);
    }

    @PostMapping("collectionQueries")
    public Object collectionQueries(@RequestBody CollectionQuery query) throws Exception {
        return queryService.executeQueryAndBroadcast(query, BroadcastType.COLLECTION);
    }

    @PostMapping("indexQueries")
    public Object indexQueries(@RequestBody IndexQuery query) throws Exception {
        return queryService.executeQueryAndBroadcast(query, BroadcastType.INDEX);
    }

    @PostMapping("documentQueries")
    public Object documentQueries(HttpServletRequest request, @RequestBody DocumentQuery query) throws Exception {
        return queryService.executeAndBroadcastDocumentQuery(request, query);
    }
}
