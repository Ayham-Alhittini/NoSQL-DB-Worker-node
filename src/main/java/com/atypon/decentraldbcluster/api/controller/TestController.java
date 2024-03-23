package com.atypon.decentraldbcluster.api.controller;

import com.atypon.decentraldbcluster.error.ResourceNotFoundException;
import com.atypon.decentraldbcluster.services.UserDetails;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/test")
@CrossOrigin("*")
public class TestController {

    private final UserDetails userDetails;
    private final ObjectMapper mapper;

    @Autowired
    public TestController(UserDetails userDetails, ObjectMapper mapper) {
        this.userDetails = userDetails;
        this.mapper = mapper;
    }

    @GetMapping
    public void test(HttpServletRequest request) throws JsonProcessingException {
        String json = """
                {
                    "field": true
                }
                """;
        JsonNode node = mapper.readTree(json);
        int x = 0;
    }
}
