package com.atypon.decentraldbcluster;

import com.atypon.decentraldbcluster.query.executors.QueryExecutor;
import com.atypon.decentraldbcluster.query.types.Query;
import com.atypon.decentraldbcluster.secuirty.JwtService;
import com.atypon.decentraldbcluster.test.builder.DocumentQueryBuilder;
import com.atypon.decentraldbcluster.utility.ListCaster;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

@SpringBootTest
public class SearchTests {

    private final String token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJIMFBiTS1WeTFmTHhpeGZaV1k1N0ZuQTFHNXRSODhJYTJieFdXeVhnMFVVIiwiaWF0IjoxNzEyNzczMTExfQ.wRSQvyWsSK9OVdK-t1NQGv8HfuVPY6ULY_VzCaLabFI";
    private final ObjectMapper mapper;
    private final JwtService jwtService;
    private final QueryExecutor queryExecutor;

    @Autowired
    public SearchTests(ObjectMapper mapper, JwtService jwtService, QueryExecutor queryExecutor) {
        this.mapper = mapper;
        this.jwtService = jwtService;
        this.queryExecutor = queryExecutor;
    }

    @Test
    public void findById() throws Exception {
        DocumentQueryBuilder builder = new DocumentQueryBuilder();

        Query query = builder
                .withOriginator( jwtService.getUserId(token) )
                .withDatabase("test")
                .withCollection("users")
                .selectDocuments()
                .withId("documentId")
                .build();

        System.out.println(queryExecutor.exec(query, JsonNode.class).toPrettyString());
    }

    @Test
    public void findAll() throws Exception {
        DocumentQueryBuilder builder = new DocumentQueryBuilder();

        Query query = builder
                .withOriginator( jwtService.getUserId(token) )
                .withDatabase("test")
                .withCollection("users")
                .selectDocuments()
                .withCondition(mapper.readTree("""
                        {
                            
                        }
                        """))
                .build();

        List<?> rawList = queryExecutor.exec(query, List.class);
        var result = ListCaster.castList(rawList, JsonNode.class);
        System.out.println("[");
        for (var i: result) System.out.println(i.toPrettyString());
        System.out.println("]");
    }
}
