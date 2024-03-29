package com.atypon.decentraldbcluster.services;

import com.atypon.decentraldbcluster.config.NodeConfiguration;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.web.client.RestTemplate;

public class BroadcastService {
    private static final String  prefixBroadcastUrl = "/internal/api/broadcast/";


    public static void doBroadcast(HttpServletRequest request, String endpoint, JsonNode body, HttpMethod method) {
        RestTemplate restTemplate = new RestTemplate();

        HttpHeaders headers = new HttpHeaders();
        headers.add("Authorization", request.getHeader("Authorization"));
        HttpEntity<JsonNode> requestEntity = new HttpEntity<>(body, headers);

        for (Integer port: NodeConfiguration.getOtherNodesPort()) {

            String requestUrl = NodeConfiguration.getNodeAddress(port) + prefixBroadcastUrl +  endpoint;

            restTemplate.exchange( requestUrl , method, requestEntity, Object.class);
        }
    }
}