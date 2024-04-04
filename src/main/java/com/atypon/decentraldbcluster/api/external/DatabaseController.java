package com.atypon.decentraldbcluster.api.external;

import com.atypon.decentraldbcluster.query.QueryExecutor;
import com.atypon.decentraldbcluster.query.base.Query;
import com.atypon.decentraldbcluster.query.databases.DatabaseQueryBuilder;
import com.atypon.decentraldbcluster.services.BroadcastService;
import com.atypon.decentraldbcluster.utility.ListCaster;
import com.atypon.decentraldbcluster.services.UserDetails;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/database")
@CrossOrigin("*")
public class DatabaseController {

    private final UserDetails userDetails;
    private final QueryExecutor queryExecutor;
    private final BroadcastService broadcastService;

    @Autowired
    public DatabaseController(UserDetails userDetails, QueryExecutor queryExecutor, BroadcastService broadcastService) {
        this.userDetails = userDetails;
        this.queryExecutor = queryExecutor;
        this.broadcastService = broadcastService;
    }

    @PostMapping("createDB/{database}")
    public void createDatabase(HttpServletRequest request, @PathVariable String database) throws Exception {

        DatabaseQueryBuilder builder = new DatabaseQueryBuilder();

        Query query = builder
                .withOriginator(userDetails.getUserId(request))
                .createDatabase(database)
                .build();

        queryExecutor.exec(query);
        broadcastService.doBroadcast(request, "database", query);
    }

    @DeleteMapping("dropDB/{database}")
    public void deleteDatabase(HttpServletRequest request, @PathVariable String database) throws Exception {

        DatabaseQueryBuilder builder = new DatabaseQueryBuilder();

        Query query = builder
                .withOriginator(userDetails.getUserId(request))
                .dropDatabase(database)
                .build();

        queryExecutor.exec(query);
        broadcastService.doBroadcast(request, "database", query);
    }

    @GetMapping("/showDbs")
    public List<String> showDbs(HttpServletRequest request) throws Exception {

        DatabaseQueryBuilder builder = new DatabaseQueryBuilder();

        Query query = builder
                .withOriginator(userDetails.getUserId(request))
                .showDbs()
                .build();

        List<?> rawList = queryExecutor.exec(query, List.class);
        return ListCaster.castList(rawList, String.class);
    }
}
