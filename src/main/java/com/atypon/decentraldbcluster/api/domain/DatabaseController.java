package com.atypon.decentraldbcluster.api.domain;

import com.atypon.decentraldbcluster.communication.braodcast.QueryBroadcastType;
import com.atypon.decentraldbcluster.entity.Database;
import com.atypon.decentraldbcluster.query.service.QueryService;
import com.atypon.decentraldbcluster.query.types.Query;
import com.atypon.decentraldbcluster.query.builder.DatabaseQueryBuilder;
import com.atypon.decentraldbcluster.security.services.JwtService;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/database")
@CrossOrigin("*")
public class DatabaseController {

    private final JwtService jwtService;
    private final QueryService queryService;

    @Autowired
    public DatabaseController(JwtService jwtService, QueryService queryService) {
        this.jwtService = jwtService;
        this.queryService = queryService;
    }

    @PostMapping("createDB")
    public void createDatabase(HttpServletRequest request, @RequestBody Database database) throws Exception {

        DatabaseQueryBuilder builder = new DatabaseQueryBuilder();
        Query query = builder
                .withOriginator(jwtService.getUserId(request))
                .createDatabase(database)
                .build();

        queryService.executeQueryAndBroadcast(query, QueryBroadcastType.DATABASE);
    }

    @DeleteMapping("dropDB/{database}")
    public void deleteDatabase(HttpServletRequest request, @PathVariable String database) throws Exception {

        DatabaseQueryBuilder builder = new DatabaseQueryBuilder();
        Query query = builder
                .withOriginator(jwtService.getUserId(request))
                .dropDatabase(database)
                .build();

        queryService.executeQueryAndBroadcast(query, QueryBroadcastType.DATABASE);
    }

    @GetMapping("/showDbs")
    public Object showDbs(HttpServletRequest request) throws Exception {

        DatabaseQueryBuilder builder = new DatabaseQueryBuilder();
        Query query = builder
                .withOriginator(jwtService.getUserId(request))
                .showDbs()
                .build();

        return queryService.executeQueryAndBroadcast(query, QueryBroadcastType.DATABASE);
    }
}
