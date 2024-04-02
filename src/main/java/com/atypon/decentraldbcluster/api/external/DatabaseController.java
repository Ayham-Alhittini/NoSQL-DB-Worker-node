package com.atypon.decentraldbcluster.api.external;

import com.atypon.decentraldbcluster.query.QueryExecutor;
import com.atypon.decentraldbcluster.query.base.Query;
import com.atypon.decentraldbcluster.query.databases.DatabaseQueryBuilder;
import com.atypon.decentraldbcluster.services.BroadcastService;
import com.atypon.decentraldbcluster.services.UserDetails;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpMethod;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/database")
@CrossOrigin("*")
public class DatabaseController {


    private final UserDetails userDetails;
    private final QueryExecutor queryExecutor;

    @Autowired
    public DatabaseController(UserDetails userDetails, QueryExecutor queryExecutor) {
        this.userDetails = userDetails;
        this.queryExecutor = queryExecutor;
    }

    @PostMapping("createDB/{database}")
    public void createDatabase(HttpServletRequest request, @PathVariable String database) throws Exception {

        DatabaseQueryBuilder builder = new DatabaseQueryBuilder();

        Query query = builder
                .withOriginator(userDetails.getUserId(request))
                .createDatabase(database)
                .build();

        queryExecutor.exec(query);
        BroadcastService.doBroadcast(request, "createDB/" + database, null, HttpMethod.POST);
    }

    @DeleteMapping("dropDB/{database}")
    public void deleteDatabase(HttpServletRequest request, @PathVariable String database) throws Exception {

        DatabaseQueryBuilder builder = new DatabaseQueryBuilder();

        Query query = builder
                .withOriginator(userDetails.getUserId(request))
                .dropDatabase(database)
                .build();

        queryExecutor.exec(query);
        BroadcastService.doBroadcast(request, "dropDB/" + database, null, HttpMethod.DELETE);
    }

    @GetMapping("/showDbs")
    public Object showDbs(HttpServletRequest request) throws Exception {

        DatabaseQueryBuilder builder = new DatabaseQueryBuilder();

        Query query = builder
                .withOriginator(userDetails.getUserId(request))
                .showDbs()
                .build();

        return queryExecutor.exec(query);
    }
}
