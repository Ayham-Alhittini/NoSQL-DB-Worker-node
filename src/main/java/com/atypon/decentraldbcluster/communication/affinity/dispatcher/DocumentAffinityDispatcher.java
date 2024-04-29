package com.atypon.decentraldbcluster.communication.affinity.dispatcher;

import com.atypon.decentraldbcluster.communication.config.NodeCommunicationConfiguration;
import com.atypon.decentraldbcluster.query.actions.DocumentAction;
import com.atypon.decentraldbcluster.query.types.DocumentQuery;
import com.atypon.decentraldbcluster.storage.managers.DocumentStorageManager;
import com.atypon.decentraldbcluster.utility.PathConstructor;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class DocumentAffinityDispatcher {
    private final DocumentStorageManager documentStorageManager;

    @Autowired
    public DocumentAffinityDispatcher(DocumentStorageManager documentStorageManager) {
        this.documentStorageManager = documentStorageManager;
    }

    public boolean shouldBeDispatchedToAffinity(DocumentQuery query, int documentAffinityPort) {
        if (query.getDocumentId() == null || !isWriteQuery(query)) return false;

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

    public int extractNodePortFromDocumentId(DocumentQuery query) throws Exception {
        String documentPath = PathConstructor.constructDocumentPath(query);
        return documentStorageManager.loadDocument(documentPath).getNodeAffinityPort();
    }

    private boolean isWriteQuery(DocumentQuery query) {
        DocumentAction action = query.getDocumentAction();
        return action != DocumentAction.ADD && action != DocumentAction.SELECT;
    }
}
