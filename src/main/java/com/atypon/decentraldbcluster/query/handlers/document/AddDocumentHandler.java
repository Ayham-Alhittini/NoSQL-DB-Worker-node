package com.atypon.decentraldbcluster.query.handlers.document;

import com.atypon.decentraldbcluster.affinity.AffinityLoadBalancer;
import com.atypon.decentraldbcluster.disk.FileSystemService;
import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.query.types.DocumentQuery;
import com.atypon.decentraldbcluster.services.DocumentIndexService;
import com.atypon.decentraldbcluster.utility.PathConstructor;
import com.atypon.decentraldbcluster.validation.DocumentValidator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class AddDocumentHandler {

    //Todo: think of make pass the id instead, and we don't need affinity port any longer actually
    private final ObjectMapper mapper;
    private final DocumentValidator documentValidator;
    private final FileSystemService fileSystemService;
    private final DocumentIndexService documentIndexService;
    private final AffinityLoadBalancer affinityLoadBalancer;

    public AddDocumentHandler(ObjectMapper mapper, DocumentValidator documentValidator, FileSystemService fileSystemService,
                              DocumentIndexService documentIndexService, AffinityLoadBalancer affinityLoadBalancer) {
        this.mapper = mapper;
        this.documentValidator = documentValidator;
        this.fileSystemService = fileSystemService;
        this.documentIndexService = documentIndexService;
        this.affinityLoadBalancer = affinityLoadBalancer;
    }

    public Document handle(DocumentQuery query) throws Exception {

        String collectionPath = PathConstructor.constructCollectionPath(query);
        documentValidator.validateDocument(query.getContent(), collectionPath, true);

        Document document = createDocumentWithOptionalAssignedId(query);
        // To broadcast with same id
        query.setDocumentId(document.getId());

        String documentPath = PathConstructor.constructDocumentPath(collectionPath, document.getId());
        saveDocument(document, documentPath);
        documentIndexService.insertToAllDocumentIndexes(document, documentPath);

        return document;
    }


    // when we add a document we broadcast it, and to guaranty it have same ID we send
    // the document ID in the query
    private Document createDocumentWithOptionalAssignedId(DocumentQuery query) {
        if (query.getDocumentId() == null) {
            return new Document(query.getContent(), affinityLoadBalancer.getNextNodeNumber());
        } else {
            return new Document(query.getContent(), query.getDocumentId());
        }
    }

    private void saveDocument(Document document, String documentPath) throws IOException {
        fileSystemService.saveFile( mapper.valueToTree(document).toPrettyString() , documentPath);
    }
}
