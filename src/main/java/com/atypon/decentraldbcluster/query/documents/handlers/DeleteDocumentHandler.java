package com.atypon.decentraldbcluster.query.documents.handlers;

import com.atypon.decentraldbcluster.affinity.AffinityLoadBalancer;
import com.atypon.decentraldbcluster.query.documents.DocumentQuery;
import com.atypon.decentraldbcluster.services.DocumentIndexService;
import com.atypon.decentraldbcluster.services.FileSystemService;
import com.atypon.decentraldbcluster.utility.PathConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DeleteDocumentHandler {
    private final FileSystemService fileSystemService;
    private final DocumentIndexService documentIndexService;
    private final AffinityLoadBalancer affinityLoadBalancer;

    @Autowired
    public DeleteDocumentHandler(FileSystemService fileSystemService, DocumentIndexService documentIndexService, AffinityLoadBalancer affinityLoadBalancer) {
        this.fileSystemService = fileSystemService;
        this.documentIndexService = documentIndexService;
        this.affinityLoadBalancer = affinityLoadBalancer;
    }

    public void handle(DocumentQuery query) throws Exception {

        String collectionPath = PathConstructor.constructCollectionPath(query);
        String documentPath = PathConstructor.constructDocumentPath(collectionPath, query.getDocument().getId());

        documentIndexService.deleteDocumentFromIndexes(documentPath);
        fileSystemService.deleteFile(documentPath);

        affinityLoadBalancer.decrementNodeAssignedDocuments(query.getDocument().getAffinityPort());
    }

}
