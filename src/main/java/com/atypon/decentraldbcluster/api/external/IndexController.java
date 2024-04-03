package com.atypon.decentraldbcluster.api.external;

import com.atypon.decentraldbcluster.query.QueryExecutor;
import com.atypon.decentraldbcluster.query.base.Query;
import com.atypon.decentraldbcluster.query.index.IndexQueryBuilder;
import com.atypon.decentraldbcluster.services.BroadcastService;
import com.atypon.decentraldbcluster.services.UserDetails;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/index")
@CrossOrigin("*")
public class IndexController {

    private final UserDetails userDetails;
    private final QueryExecutor queryExecutor;

    @Autowired
    public IndexController(UserDetails userDetails, QueryExecutor queryExecutor) {
        this.userDetails = userDetails;
        this.queryExecutor = queryExecutor;
    }

    @PostMapping("createIndex/{database}/{collection}/{field}")
    public void createIndex(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @PathVariable String field) throws Exception {

        IndexQueryBuilder builder = new IndexQueryBuilder();

        Query query = builder
                .withOriginator( userDetails.getUserId(request) )
                .withDatabase(database)
                .withCollection(collection)
                .createIndex(field)
                .build();

        queryExecutor.exec(query);
        BroadcastService.doBroadcast(request, "index", query);
    }

    @DeleteMapping("dropIndex/{database}/{collection}/{field}")
    public void deleteIndex(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @PathVariable String field) throws Exception {

        IndexQueryBuilder builder = new IndexQueryBuilder();

        Query query = builder
                .withOriginator( userDetails.getUserId(request) )
                .withDatabase(database)
                .withCollection(collection)
                .dropIndex(field)
                .build();

        queryExecutor.exec(query);
        BroadcastService.doBroadcast(request, "index", query);
    }

}
