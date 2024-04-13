package com.atypon.decentraldbcluster;

import com.atypon.decentraldbcluster.query.executors.QueryExecutor;
import com.atypon.decentraldbcluster.query.types.Query;
import com.atypon.decentraldbcluster.secuirty.JwtService;
import com.atypon.decentraldbcluster.test.builder.DatabaseQueryBuilder;
import com.atypon.decentraldbcluster.utility.ListCaster;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

@SpringBootTest
class DatabaseTests {

    private final String token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJIMFBiTS1WeTFmTHhpeGZaV1k1N0ZuQTFHNXRSODhJYTJieFdXeVhnMFVVIiwiaWF0IjoxNzEyNzczMTExfQ.wRSQvyWsSK9OVdK-t1NQGv8HfuVPY6ULY_VzCaLabFI";
    private final JwtService jwtService;
    private final QueryExecutor queryExecutor;

    @Autowired
    DatabaseTests(JwtService jwtService, QueryExecutor queryExecutor) {
        this.jwtService = jwtService;
        this.queryExecutor = queryExecutor;
    }

    @Test
    public void createDb() throws Exception {
        DatabaseQueryBuilder builder = new DatabaseQueryBuilder();

        Query query = builder
                .withOriginator(jwtService.getUserId(token))
                .createDatabase("test")
                .build();

        queryExecutor.exec(query);
    }

    @Test
    public void dropDb() throws Exception {
        DatabaseQueryBuilder builder = new DatabaseQueryBuilder();

        Query query = builder
                .withOriginator(jwtService.getUserId(token))
                .dropDatabase("test")
                .build();

        queryExecutor.exec(query);
    }

    @Test
    public void showDbs() throws Exception {
        DatabaseQueryBuilder builder = new DatabaseQueryBuilder();

        Query query = builder
                .withOriginator(jwtService.getUserId(token))
                .showDbs()
                .build();

        List<?> rawList = queryExecutor.exec(query, List.class);
        System.out.println(ListCaster.castList(rawList, String.class));
    }

}
