package com.atypon.decentraldbcluster.query.documents;

import com.atypon.decentraldbcluster.affinity.AffinityLoadBalancer;
import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.index.Index;
import com.atypon.decentraldbcluster.index.IndexManager;
import com.atypon.decentraldbcluster.query.base.Executable;
import com.atypon.decentraldbcluster.services.*;
import com.atypon.decentraldbcluster.validation.DocumentValidator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;

@Component
public class DocumentQueryExecutor implements Executable<DocumentQuery> {

    private final ObjectMapper mapper;
    private final IndexManager indexManager;
    private final DocumentFilterService filterService;
    private final DocumentValidator documentValidator;
    private final FileSystemService fileSystemService;
    private final DocumentIndexService documentIndexService;
    private final AffinityLoadBalancer affinityLoadBalancer;
    private final DocumentReaderService documentReaderService;

    @Autowired
    public DocumentQueryExecutor(FileSystemService fileSystemService, DocumentIndexService documentIndexService,
                                 ObjectMapper mapper, DocumentReaderService documentReaderService, DocumentValidator documentValidator, AffinityLoadBalancer affinityLoadBalancer, DocumentFilterService queryService, IndexManager indexManager) {
        this.mapper = mapper;
        this.filterService = queryService;
        this.indexManager = indexManager;
        this.documentValidator = documentValidator;
        this.fileSystemService = fileSystemService;
        this.documentIndexService = documentIndexService;
        this.affinityLoadBalancer = affinityLoadBalancer;
        this.documentReaderService = documentReaderService;
    }

    @Override
    public Object exec(DocumentQuery query) throws Exception {
        return switch (query.getDocumentAction()) {
            case ADD -> handleAddDocument(query);
            case DELETE -> handleDeleteDocument(query);
            case UPDATE -> handleUpdateDocument(query);
            case SELECT -> handleSelectDocuments(query);
        };
    }

    private Object handleAddDocument(DocumentQuery query) throws Exception {
        Document document = new Document(query.getContent(), affinityLoadBalancer.getNextAffinityNodePort());

        String collectionPath = PathConstructor
                .constructCollectionPath(query.getOriginator(), query.getDatabase(), query.getCollection());
        String documentPath = PathConstructor.constructDocumentPath(collectionPath, document.getId());

        documentValidator.doesDocumentMatchSchema(query.getContent(), collectionPath, true);

        fileSystemService.saveFile( mapper.valueToTree(document).toPrettyString() , documentPath);
        documentIndexService.insertToAllIndexes(document, documentPath);

        return document;
    }


    private Object handleDeleteDocument(DocumentQuery query) throws Exception {

        String collectionPath = PathConstructor
                .constructCollectionPath(query.getOriginator(), query.getDatabase(), query.getCollection());
        String documentPath = PathConstructor.constructDocumentPath(collectionPath, query.getDocument().getId());

        documentIndexService.deleteDocumentFromIndexes(documentPath);
        fileSystemService.deleteFile(documentPath);

        affinityLoadBalancer.decrementNodeAssignedDocuments(query.getDocument().getAffinityPort());
        return null;
    }


    private Object handleUpdateDocument(DocumentQuery query) throws Exception {
        Document document = query.getDocument();
        String collectionPath = PathConstructor.constructCollectionPath(query.getOriginator(), query.getDatabase(), query.getCollection());
        String documentPath = PathConstructor.constructDocumentPath(collectionPath, document.getId());

        documentValidator.doesDocumentMatchSchema(query.getNewContent(), collectionPath, false);

        JsonNode updatedDocumentData = documentReaderService.patchDocument(query.getNewContent(), document.getData());
        document.setData(updatedDocumentData);
        document.incrementVersion();

        documentIndexService.updateIndexes(document, query.getNewContent(), collectionPath);
        fileSystemService.saveFile(mapper.valueToTree(document).toPrettyString(), documentPath);

        return document;
    }
    private Object handleSelectDocuments(DocumentQuery query) throws Exception {

        String collectionPath = PathConstructor.constructCollectionPath(query.getOriginator(), query.getDatabase(), query.getCollection());

        if (query.getDocumentId() != null)
            return filterService.findDocumentById(collectionPath, query.getDocumentId());

        String mostSelectiveIndexField = filterService.getMostSelectiveIndexFiled(query.getCondition(), collectionPath);
        //No index exists, need to filter all the documents
        if (mostSelectiveIndexField == null) {
            List<Document> documents = documentReaderService.readDocumentsByCollectionPath(collectionPath);
            return filterService.filterDocuments(documents, query.getCondition());
        }

        String indexPath = PathConstructor.constructUserGeneratedIndexPath(collectionPath, mostSelectiveIndexField);
        Index mostSelectiveIndex = indexManager.loadIndex(indexPath);
        Set<String> indexPointers = mostSelectiveIndex.getPointers( query.getCondition().get(mostSelectiveIndexField) );
        List<Document> documents = documentReaderService.readDocumentsByDocumentsPathList(indexPointers);

        return filterService.filterDocuments(documents, query.getCondition());
    }




    //Todo: think of moving some of the logic to the services
}
