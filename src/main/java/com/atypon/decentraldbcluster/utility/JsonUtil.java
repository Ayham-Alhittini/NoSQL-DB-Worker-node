package com.atypon.decentraldbcluster.utility;

import com.atypon.decentraldbcluster.document.Document;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class JsonUtil {
    private final ObjectMapper mapper;

    public JsonUtil(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public String toJsonString(Document document) throws IOException {
        return mapper.writeValueAsString(document);
    }

    public Document fromJsonString(String json) throws IOException {
        return mapper.readValue(json, Document.class);
    }
}
