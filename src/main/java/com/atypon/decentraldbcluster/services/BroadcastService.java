package com.atypon.decentraldbcluster.services;

import com.atypon.decentraldbcluster.config.NodeConfiguration;
import com.atypon.decentraldbcluster.query.base.Query;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class BroadcastService {

    //TODO: consider async broadcast
    public void doBroadcast(HttpServletRequest request, String endpoint, Query query) {
        RestTemplate restTemplate = new RestTemplate();

        HttpHeaders headers = new HttpHeaders();
        headers.add("Authorization", request.getHeader("Authorization"));
        HttpEntity<Object> requestEntity = new HttpEntity<>(query, headers);

        for (Integer port: NodeConfiguration.getOtherNodesPort()) {

            String prefixBroadcastUrl = "/internal/api/broadcast/";
            String requestUrl = NodeConfiguration.getNodeAddress(port) + prefixBroadcastUrl +  endpoint;

            restTemplate.exchange( requestUrl , HttpMethod.POST, requestEntity, Void.class);
        }
    }
}