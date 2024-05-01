package com.atypon.decentraldbcluster.communication.braodcast;

import com.atypon.decentraldbcluster.communication.config.NodeCommunicationConfiguration;
import com.atypon.decentraldbcluster.query.types.Query;
import com.atypon.decentraldbcluster.security.services.NodeAuthorizationSecretEncryption;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.MediaType;

import java.io.IOException;

@Service
public class BroadcastService {

    private final NodeAuthorizationSecretEncryption nodeAuthorizationSecretEncryption;

    @Autowired
    public BroadcastService(NodeAuthorizationSecretEncryption nodeAuthorizationSecretEncryption) {
        this.nodeAuthorizationSecretEncryption = nodeAuthorizationSecretEncryption;
    }

    public void doQueryBroadcastForWriteQuery(QueryBroadcastType broadcastType, Query query) {

        if (!query.isWriteQuery())return;
        String endpoint = broadcastType.toString().toLowerCase();
        query.setBroadcastQuery(true);
        HttpHeaders headers = new HttpHeaders();

        // A custom header for guaranteed only nodes between each other can broadcast
        headers.add("X-Node-Authorization", nodeAuthorizationSecretEncryption.getNodeAuthenticationKey());

        RestTemplate restTemplate = new RestTemplate();
        HttpEntity<Object> requestEntity = new HttpEntity<>(query, headers);

        for (Integer port: NodeCommunicationConfiguration.getOtherNodesPort()) {

            String prefixBroadcastUrl = "/internal/api/broadcast/";
            String requestUrl = NodeCommunicationConfiguration.getNodeAddress(port) + prefixBroadcastUrl +  endpoint;

            restTemplate.exchange(requestUrl, HttpMethod.POST, requestEntity, Void.class);
        }
    }

    public void doBackupBroadcast(MultipartFile backupFile) throws IOException {
        HttpHeaders headers = new HttpHeaders();
        headers.add("X-Node-Authorization", nodeAuthorizationSecretEncryption.getNodeAuthenticationKey());
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);

        MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();
        body.add("backupFile", new ByteArrayResource(backupFile.getBytes()) {
            @Override
            public String getFilename() {
                return "backupFile.zip";
            }
        });
        body.add("filename", "backupFile.zip");

        HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(body, headers);

        RestTemplate restTemplate = new RestTemplate();

        for (Integer port : NodeCommunicationConfiguration.getOtherNodesPort()) {
            String prefixBroadcastUrl = "/internal/api/broadcast/";
            String requestUrl = NodeCommunicationConfiguration.getNodeAddress(port) + prefixBroadcastUrl + "backup";
            restTemplate.exchange(requestUrl, HttpMethod.POST, requestEntity, Void.class);
        }
    }

}
