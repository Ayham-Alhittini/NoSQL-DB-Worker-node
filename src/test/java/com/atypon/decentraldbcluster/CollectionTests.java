package com.atypon.decentraldbcluster;

import com.atypon.decentraldbcluster.query.executors.QueryExecutor;
import com.atypon.decentraldbcluster.query.types.Query;
import com.atypon.decentraldbcluster.secuirty.JwtService;
import com.atypon.decentraldbcluster.test.builder.CollectionQueryBuilder;
import com.atypon.decentraldbcluster.utility.ListCaster;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

@SpringBootTest
public class CollectionTests {
    private final String token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJIMFBiTS1WeTFmTHhpeGZaV1k1N0ZuQTFHNXRSODhJYTJieFdXeVhnMFVVIiwiaWF0IjoxNzEyNzczMTExfQ.wRSQvyWsSK9OVdK-t1NQGv8HfuVPY6ULY_VzCaLabFI";
    private final ObjectMapper mapper;
    private final JwtService jwtService;
    private final QueryExecutor queryExecutor;

    @Autowired
    public CollectionTests(ObjectMapper mapper, JwtService jwtService, QueryExecutor queryExecutor) {
        this.mapper = mapper;
        this.jwtService = jwtService;
        this.queryExecutor = queryExecutor;
    }

    @Test
    public void createCollection() throws Exception {
        CollectionQueryBuilder builder = new CollectionQueryBuilder();

        Query query = builder
                .withOriginator(jwtService.getUserId(token))
                .withDatabase("test")
                .createCollection("users")
                .withSchema(mapper.readTree("""
                        {
                          "age": "INTEGER",
                          "tail": "DECIMAL",
                          "siblings": "ARRAY",
                          "isAdult": "BOOLEAN",
                          "name" : "STRING",
                          "gender" : "STRING",
                          "email" : "STRING",
                          "bornDate": "DATETIME",
                          "address": {
                               "street": "STRING",
                               "city": "STRING",
                               "state": "STRING",
                               "zipCode": "STRING",
                               "test": {"testFiled": "BOOLEAN"}
                            }
                        }
                        """))
                .build();

        queryExecutor.exec(query);
    }

    @Test
    public void createSchemaLessCollection() throws Exception {
        CollectionQueryBuilder builder = new CollectionQueryBuilder();

        Query query = builder
                .withOriginator(jwtService.getUserId(token))
                .withDatabase("test")
                .createCollection("users")
                .build();

        queryExecutor.exec(query);
    }

    @Test
    public void dropCollection() throws Exception {
        CollectionQueryBuilder builder = new CollectionQueryBuilder();

        Query query = builder
                .withOriginator(jwtService.getUserId(token))
                .withDatabase("test")
                .dropCollection("users")
                .build();

        queryExecutor.exec(query);
    }

    @Test
    public void showCollections() throws Exception {
        CollectionQueryBuilder builder = new CollectionQueryBuilder();

        Query query = builder
                .withOriginator(jwtService.getUserId(token))
                .withDatabase("test")
                .showCollections()
                .build();

        List<?> rawList = queryExecutor.exec(query, List.class);
        System.out.println(ListCaster.castList(rawList, String.class));
    }

}
