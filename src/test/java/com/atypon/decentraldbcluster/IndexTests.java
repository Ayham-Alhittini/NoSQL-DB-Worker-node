package com.atypon.decentraldbcluster;

import com.atypon.decentraldbcluster.query.executors.QueryExecutor;
import com.atypon.decentraldbcluster.query.types.Query;
import com.atypon.decentraldbcluster.secuirty.JwtService;
import com.atypon.decentraldbcluster.test.builder.IndexQueryBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class IndexTests {
    private final String token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJIMFBiTS1WeTFmTHhpeGZaV1k1N0ZuQTFHNXRSODhJYTJieFdXeVhnMFVVIiwiaWF0IjoxNzEyNzczMTExfQ.wRSQvyWsSK9OVdK-t1NQGv8HfuVPY6ULY_VzCaLabFI";
    private final JwtService jwtService;
    private final QueryExecutor queryExecutor;

    @Autowired
    public IndexTests(JwtService jwtService, QueryExecutor queryExecutor) {
        this.jwtService = jwtService;
        this.queryExecutor = queryExecutor;
    }

    @Test
    public void createIndex() throws Exception {
        IndexQueryBuilder builder = new IndexQueryBuilder();

        Query query = builder
                .withOriginator( jwtService.getUserId(token) )
                .withDatabase("test")
                .withCollection("users")
                .createIndex("gender")
                .build();

        queryExecutor.exec(query);
    }

    @Test
    public void deleteIndex() throws Exception {
        IndexQueryBuilder builder = new IndexQueryBuilder();

        Query query = builder
                .withOriginator( jwtService.getUserId(token) )
                .withDatabase("test")
                .withCollection("users")
                .dropIndex("gender")
                .build();

        queryExecutor.exec(query);
    }
}
