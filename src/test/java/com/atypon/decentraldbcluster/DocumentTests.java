package com.atypon.decentraldbcluster;

import com.atypon.decentraldbcluster.query.executors.DocumentQueryExecutor;
import com.atypon.decentraldbcluster.query.executors.QueryExecutor;
import com.atypon.decentraldbcluster.query.types.DocumentQuery;
import com.atypon.decentraldbcluster.query.types.Query;
import com.atypon.decentraldbcluster.secuirty.JwtService;
import com.atypon.decentraldbcluster.test.builder.DocumentQueryBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class DocumentTests {
    private final String token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJIMFBiTS1WeTFmTHhpeGZaV1k1N0ZuQTFHNXRSODhJYTJieFdXeVhnMFVVIiwiaWF0IjoxNzEyNzczMTExfQ.wRSQvyWsSK9OVdK-t1NQGv8HfuVPY6ULY_VzCaLabFI";
    private final ObjectMapper mapper;
    private final JwtService jwtService;
    private final QueryExecutor queryExecutor;
    private final DocumentQueryExecutor documentQueryExecutor;

    @Autowired
    public DocumentTests(ObjectMapper mapper, JwtService jwtService, QueryExecutor queryExecutor, DocumentQueryExecutor documentQueryExecutor) {
        this.mapper = mapper;
        this.jwtService = jwtService;
        this.queryExecutor = queryExecutor;
        this.documentQueryExecutor = documentQueryExecutor;
    }

    @Test
    public void addDocument() throws Exception {
        DocumentQueryBuilder builder = new DocumentQueryBuilder();
        DocumentQuery query = builder
                .withOriginator(jwtService.getUserId(token))
                .withDatabase("test")
                .withCollection("users")
                .addDocument(mapper.readTree("""
                        {
                          "age" : 17,
                          "tail" : 156,
                          "siblings" : [ "ayham", "mulham", "alma" ],
                          "isAdult" : true,
                          "name" : "Menas alhettini",
                          "gender" : "Female",
                          "email" : "menas.hittini@example.com",
                          "bornDate" : "2006-12-29 10:00:00",
                          "address" : {
                            "street" : "alqsa",
                            "city" : "zarqa",
                            "state" : "zarqa",
                            "zipCode" : "13303",
                            "test" : {
                              "testFiled" : false
                            }
                          }
                        }
                        """))
                .build();

        JsonNode result = queryExecutor.exec(query, JsonNode.class);

        System.out.println(result.toPrettyString());
    }

    @Test
    public void deleteDocument() throws Exception {
        DocumentQueryBuilder builder = new DocumentQueryBuilder();
        Query query = builder
                .withOriginator(jwtService.getUserId(token))
                .withDatabase("test")
                .withCollection("users")
                .deleteDocument("documentId")
                .build();

        queryExecutor.exec(query);
    }

    @Test
    public void updateDocument() throws Exception {
        DocumentQueryBuilder builder = new DocumentQueryBuilder();
        DocumentQuery query = builder
                .withOriginator(jwtService.getUserId(token))
                .withDatabase("test")
                .withCollection("users")
                .updateDocument("documentId", mapper.readTree("""
                        {
                            "gender": "Male"
                        }
                        """))
                .build();

        var result = documentQueryExecutor.execWithOptimisticLockingForModify(query);
        System.out.println(((JsonNode) result).toPrettyString());
    }

    @Test
    public void replaceDocument() throws Exception {
        DocumentQueryBuilder builder = new DocumentQueryBuilder();
        DocumentQuery query = builder
                .withOriginator(jwtService.getUserId(token))
                .withDatabase("test")
                .withCollection("users")
                .replaceDocument("b6ad8cd5-3d7d-4cbe-b368-8d3161f93c931", mapper.readTree("""
                        {"object_id":"b6ad8cd5-3d7d-4cbe-b368-8d3161f93c931","age":17,"tail":156,"siblings":["ayham","mulham","alma"],"isAdult":true,"name":"Menas alhettini","gender":"Female","email":"menas.hittini@example.com","bornDate":"2006-12-29 10:00:00","address":{"street":"alqsa","city":"zarqa","state":"zarqa","zipCode":"13303","test":{"testFiled":true}}}
                        """))
                .build();

        var result = documentQueryExecutor.execWithOptimisticLockingForModify(query);
        System.out.println(((JsonNode) result).toPrettyString());
    }


}
