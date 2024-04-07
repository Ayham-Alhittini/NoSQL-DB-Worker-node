package com.atypon.decentraldbcluster.query.documents.handlers;

import com.atypon.decentraldbcluster.affinity.AffinityLoadBalancer;
import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.query.documents.DocumentQuery;
import com.atypon.decentraldbcluster.services.DocumentIndexService;
import com.atypon.decentraldbcluster.disk.FileSystemService;
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

        Document document = getOrCreateDocumentBasedOnQuery(query);

        String documentPath = PathConstructor.constructDocumentPath(collectionPath, document.getId());
        saveDocument(document, documentPath);
        documentIndexService.insertToAllDocumentIndexes(document, documentPath);

        return document;
    }


    // when we add a document we broadcast it, and to guaranty it have same ID, and affinity port we send
    // the document itself in the query
    private Document getOrCreateDocumentBasedOnQuery(DocumentQuery query) {
        Document queryDocument = query.getDocument();
        if (queryDocument == null) {
            return new Document(query.getContent(), affinityLoadBalancer.getNextAffinityNodePort());
        } else {
            affinityLoadBalancer.incrementNodeAssignedDocuments(queryDocument.getAffinityPort());
            return queryDocument;
        }
    }




    private void saveDocument(Document document, String documentPath) throws IOException {
        fileSystemService.saveFile( mapper.valueToTree(document).toPrettyString() , documentPath);
    }
}
