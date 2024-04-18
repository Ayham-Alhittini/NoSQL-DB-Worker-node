package com.atypon.decentraldbcluster.test.api;

import com.atypon.decentraldbcluster.query.executors.QueryExecutor;
import com.atypon.decentraldbcluster.query.types.Query;
import com.atypon.decentraldbcluster.test.builder.DatabaseQueryBuilder;
import com.atypon.decentraldbcluster.security.services.JwtService;
import com.atypon.decentraldbcluster.communication.braodcast.BroadcastService;
import com.atypon.decentraldbcluster.utility.ListCaster;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/database")
@CrossOrigin("*")
public class DatabaseController {

    private final JwtService jwtService;
    private final QueryExecutor queryExecutor;
    private final BroadcastService broadcastService;

    @Autowired
    public DatabaseController(JwtService jwtService, QueryExecutor queryExecutor, BroadcastService broadcastService) {
        this.jwtService = jwtService;
        this.queryExecutor = queryExecutor;
        this.broadcastService = broadcastService;
    }

    @PostMapping("createDB/{database}")
    public void createDatabase(HttpServletRequest request, @PathVariable String database) throws Exception {

        DatabaseQueryBuilder builder = new DatabaseQueryBuilder();

        Query query = builder
                .withOriginator(jwtService.getUserId(request))
                .createDatabase(database)
                .build();

        queryExecutor.exec(query);
        broadcastService.doBroadcast("database", query);
    }

    @DeleteMapping("dropDB/{database}")
    public void deleteDatabase(HttpServletRequest request, @PathVariable String database) throws Exception {

        DatabaseQueryBuilder builder = new DatabaseQueryBuilder();

        Query query = builder
                .withOriginator(jwtService.getUserId(request))
                .dropDatabase(database)
                .build();

        queryExecutor.exec(query);
        broadcastService.doBroadcast("database", query);
    }

    @GetMapping("/showDbs")
    public List<String> showDbs(HttpServletRequest request) throws Exception {

        DatabaseQueryBuilder builder = new DatabaseQueryBuilder();

        Query query = builder
                .withOriginator(jwtService.getUserId(request))
                .showDbs()
                .build();

        List<?> rawList = queryExecutor.exec(query, List.class);
        return ListCaster.castList(rawList, String.class);
    }
}
