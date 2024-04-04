package com.atypon.decentraldbcluster.query.documents.handlers;

import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.index.Index;
import com.atypon.decentraldbcluster.index.IndexManager;
import com.atypon.decentraldbcluster.query.documents.DocumentQuery;
import com.atypon.decentraldbcluster.services.DocumentFilterService;
import com.atypon.decentraldbcluster.services.DocumentReaderService;
import com.atypon.decentraldbcluster.utility.PathConstructor;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.Set;

@Service
public class SelectDocumentsHandler {
    private final IndexManager indexManager;
    private final DocumentFilterService filterService;
    private final DocumentReaderService documentReaderService;

    public SelectDocumentsHandler(IndexManager indexManager, DocumentFilterService filterService, DocumentReaderService documentReaderService) {
        this.indexManager = indexManager;
        this.filterService = filterService;
        this.documentReaderService = documentReaderService;
    }

    public Object handle(DocumentQuery query) throws Exception {

        String collectionPath = PathConstructor.constructCollectionPath(query);

        if (isSelectByIdQuery(query)) {
            return documentReaderService.findDocumentById(collectionPath, query.getDocumentId());
        } else {
            return selectByContent(query, collectionPath);
        }
    }

    private boolean isSelectByIdQuery(DocumentQuery query) {
        return query.getDocumentId() != null;
    }

    private List<Document> selectByContent(DocumentQuery query, String collectionPath) throws Exception {

        String mostSelectiveIndexField = filterService.getMostSelectiveIndexFiled(query.getCondition(), collectionPath);

        if (isIndexesEmpty(mostSelectiveIndexField)) {
            return filterAllCollectionDocuments(query.getCondition(), collectionPath);
        } else {
            return filterDocumentsFromMostSelectiveIndex(mostSelectiveIndexField, query.getCondition(), collectionPath);
        }
    }

    private boolean isIndexesEmpty(String mostSelectiveIndexField) {
        return mostSelectiveIndexField == null;
    }

    private List<Document> filterAllCollectionDocuments(JsonNode condition, String collectionPath) throws IOException {
        List<Document> documents = documentReaderService.readDocumentsByCollectionPath(collectionPath);
        return filterService.filterDocuments(documents, condition);
    }

    private List<Document> filterDocumentsFromMostSelectiveIndex(String mostSelectiveIndexField, JsonNode condition, String collectionPath) throws Exception {
        String indexPath = PathConstructor.constructUserGeneratedIndexPath(collectionPath, mostSelectiveIndexField);
        Index mostSelectiveIndex = indexManager.loadIndex(indexPath);
        Set<String> indexPointers = mostSelectiveIndex.getPointers( condition.get(mostSelectiveIndexField) );
        List<Document> documents = documentReaderService.readDocumentsByDocumentsPathList(indexPointers);

        return filterService.filterDocuments(documents, condition);
    }
}
