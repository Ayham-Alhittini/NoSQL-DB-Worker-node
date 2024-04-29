package com.atypon.decentraldbcluster.communication.braodcast;

import com.atypon.decentraldbcluster.communication.config.NodeCommunicationConfiguration;
import com.atypon.decentraldbcluster.query.types.Query;
import com.atypon.decentraldbcluster.security.services.NodeAuthorizationSecretEncryption;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class BroadcastService {

    private final NodeAuthorizationSecretEncryption nodeAuthorizationSecretEncryption;

    @Autowired
    public BroadcastService(NodeAuthorizationSecretEncryption nodeAuthorizationSecretEncryption) {
        this.nodeAuthorizationSecretEncryption = nodeAuthorizationSecretEncryption;
    }

    public void doBroadcast(BroadcastType broadcastType, Query query) {
        String endpoint = broadcastType.toString().toLowerCase();
        query.setBroadcastQuery(true);

        RestTemplate restTemplate = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();

        // A custom header for guaranteed only nodes between each other can broadcast
        headers.add("X-Node-Authorization", nodeAuthorizationSecretEncryption.getNodeAuthenticationKey());

        HttpEntity<Object> requestEntity = new HttpEntity<>(query, headers);

        for (Integer port: NodeCommunicationConfiguration.getOtherNodesPort()) {

            String prefixBroadcastUrl = "/internal/api/broadcast/";
            String requestUrl = NodeCommunicationConfiguration.getNodeAddress(port) + prefixBroadcastUrl +  endpoint;

            restTemplate.exchange(requestUrl, HttpMethod.POST, requestEntity, Void.class);
        }
    }
}
