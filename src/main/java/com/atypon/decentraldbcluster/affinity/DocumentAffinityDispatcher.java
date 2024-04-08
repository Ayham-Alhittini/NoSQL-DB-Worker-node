package com.atypon.decentraldbcluster.affinity;

import com.atypon.decentraldbcluster.config.NodeConfiguration;
import com.atypon.decentraldbcluster.query.actions.DocumentAction;
import com.atypon.decentraldbcluster.query.types.DocumentQuery;
import com.atypon.decentraldbcluster.utility.DocumentUtil;
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
        int queryNodePort = DocumentUtil.extractNodePortFromDocumentId(query.getDocumentId());
        int currentNodePort = NodeConfiguration.getCurrentNodePort();

        DocumentAction action = query.getDocumentAction();

        return  (action != DocumentAction.SELECT && action != DocumentAction.ADD) && queryNodePort != currentNodePort;
    }

    public Object dispatchToAffinity(HttpServletRequest request, DocumentQuery query) {
        int affinityPort = DocumentUtil.extractNodePortFromDocumentId(query.getDocumentId());
        String affinityUrl = NodeConfiguration.getNodeAddress(affinityPort) + "/api/query/documentQueries";

        RestTemplate restTemplate = new RestTemplate();

        HttpHeaders headers = new HttpHeaders();
        headers.add("Authorization", request.getHeader("Authorization"));
        HttpEntity<Object> requestEntity = new HttpEntity<>(query, headers);

        return restTemplate.exchange( affinityUrl , HttpMethod.POST, requestEntity, Object.class);
    }


}
