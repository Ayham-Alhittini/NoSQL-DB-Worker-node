package com.atypon.decentraldbcluster.communication.affinity.dispatcher;

import com.atypon.decentraldbcluster.communication.config.NodeCommunicationConfiguration;
import com.atypon.decentraldbcluster.query.actions.DocumentAction;
import com.atypon.decentraldbcluster.query.types.DocumentQuery;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class DocumentAffinityDispatcher {

    public boolean isAffinityRelatedQuery(DocumentQuery query) {
        return query.isWriteQuery() && query.getDocumentAction() != DocumentAction.ADD;
    }

    public boolean shouldBeDispatchedToAffinity(int documentAffinityPort) {
        int currentPort = NodeCommunicationConfiguration.getCurrentNodePort();
        return documentAffinityPort != currentPort;
    }

    public Object dispatchToAffinity(HttpServletRequest request, DocumentQuery query, int affinityPort) {
        String affinityUrl = NodeCommunicationConfiguration.getNodeAddress(affinityPort) + "/api/query/documentQueries";

        RestTemplate restTemplate = new RestTemplate();

        HttpHeaders headers = new HttpHeaders();
        headers.add("Authorization", request.getHeader("Authorization"));
        HttpEntity<Object> requestEntity = new HttpEntity<>(query, headers);

        return restTemplate.exchange( affinityUrl , HttpMethod.POST, requestEntity, Object.class);
    }
}
