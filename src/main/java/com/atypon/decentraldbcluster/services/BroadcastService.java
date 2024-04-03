package com.atypon.decentraldbcluster.services;

import com.atypon.decentraldbcluster.config.NodeConfiguration;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.web.client.RestTemplate;

public class BroadcastService {
    private static final String  prefixBroadcastUrl = "/internal/api/broadcast/";


    public static void doBroadcast(HttpServletRequest request, String endpoint, Object body) {
        RestTemplate restTemplate = new RestTemplate();

        HttpHeaders headers = new HttpHeaders();
        headers.add("Authorization", request.getHeader("Authorization"));
        HttpEntity<Object> requestEntity = new HttpEntity<>(body, headers);

        for (Integer port: NodeConfiguration.getOtherNodesPort()) {

            String requestUrl = NodeConfiguration.getNodeAddress(port) + prefixBroadcastUrl +  endpoint;

            restTemplate.exchange( requestUrl , HttpMethod.POST, requestEntity, Void.class);
        }
    }
}