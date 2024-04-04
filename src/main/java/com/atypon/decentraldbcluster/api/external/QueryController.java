package com.atypon.decentraldbcluster.api.external;

import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.query.QueryExecutor;
import com.atypon.decentraldbcluster.query.base.Query;
import com.atypon.decentraldbcluster.query.documents.DocumentQueryBuilder;
import com.atypon.decentraldbcluster.utility.ListCaster;
import com.atypon.decentraldbcluster.services.UserDetails;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/query")
@CrossOrigin("*")
public class QueryController {

    private final UserDetails userDetails;
    private final QueryExecutor queryExecutor;

    @Autowired
    public QueryController(UserDetails userDetails, QueryExecutor queryExecutor) {
        this.userDetails = userDetails;
        this.queryExecutor = queryExecutor;
    }

    @GetMapping("findById/{database}/{collection}/{documentId}")
    public Document findById(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @PathVariable String documentId) throws Exception {

        DocumentQueryBuilder builder = new DocumentQueryBuilder();

        Query query = builder
                .withOriginator( userDetails.getUserId(request) )
                .withDatabase(database)
                .withCollection(collection)
                .selectDocuments()
                .withId(documentId)
                .build();

        return queryExecutor.exec(query, Document.class);
    }


    @GetMapping("{database}/{collection}/find")
    public List<Document> find(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @RequestBody JsonNode filter) throws Exception {

        DocumentQueryBuilder builder = new DocumentQueryBuilder();

        Query query = builder
                .withOriginator( userDetails.getUserId(request) )
                .withDatabase(database)
                .withCollection(collection)
                .selectDocuments()
                .withCondition(filter)
                .build();

        List<?> rawList = queryExecutor.exec(query, List.class);
        return ListCaster.castList(rawList, Document.class);
    }

}
