package com.atypon.decentraldbcluster.communication.affinity;

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

    public boolean shouldBeDispatchedToAffinity(DocumentQuery query) {
        if (query.getDocumentId() == null) return false;
        int documentAffinityPort = extractNodePortFromDocumentId(query.getDocumentId());
        int currentPort = NodeCommunicationConfiguration.getCurrentNodePort();

        return  isWriteQuery(query) && (documentAffinityPort != currentPort);
    }

    public Object dispatchToAffinity(HttpServletRequest request, DocumentQuery query) {
        int affinityPort = extractNodePortFromDocumentId(query.getDocumentId());
        String affinityUrl = NodeCommunicationConfiguration.getNodeAddress(affinityPort) + "/api/query/documentQueries";

        RestTemplate restTemplate = new RestTemplate();

        HttpHeaders headers = new HttpHeaders();
        headers.add("Authorization", request.getHeader("Authorization"));
        HttpEntity<Object> requestEntity = new HttpEntity<>(query, headers);

        return restTemplate.exchange( affinityUrl , HttpMethod.POST, requestEntity, Object.class);
    }

    private int extractNodePortFromDocumentId(String documentId) {
        int basePort = 8080;
        // The last digit in the ID represents the node number
        int nodeNumber = documentId.charAt(documentId.length() - 1) - '0';
        return basePort + nodeNumber;
    }

    private boolean isWriteQuery(DocumentQuery query) {
        DocumentAction action = query.getDocumentAction();
        return action != DocumentAction.ADD && action != DocumentAction.SELECT;
    }


}
