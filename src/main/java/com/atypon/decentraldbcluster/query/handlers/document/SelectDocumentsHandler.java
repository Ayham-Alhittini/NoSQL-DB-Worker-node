package com.atypon.decentraldbcluster.query.handlers.document;

import com.atypon.decentraldbcluster.document.entity.Document;
import com.atypon.decentraldbcluster.document.services.DocumentFilterService;
import com.atypon.decentraldbcluster.document.services.DocumentQueryService;
import com.atypon.decentraldbcluster.index.Index;
import com.atypon.decentraldbcluster.persistence.IndexPersistenceManager;
import com.atypon.decentraldbcluster.query.types.DocumentQuery;
import com.atypon.decentraldbcluster.utility.PathConstructor;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Set;

@Service
public class SelectDocumentsHandler {
    //Todo: too many logic for document handler, suppose move the logic for document services
    private final DocumentFilterService filterService;
    private final DocumentQueryService documentQueryService;
    private final IndexPersistenceManager indexPersistenceManager;

    public SelectDocumentsHandler(DocumentFilterService filterService, DocumentQueryService documentQueryService, IndexPersistenceManager indexPersistenceManager) {
        this.filterService = filterService;
        this.documentQueryService = documentQueryService;
        this.indexPersistenceManager = indexPersistenceManager;
    }


    public Object handle(DocumentQuery query) throws Exception {

        String collectionPath = PathConstructor.constructCollectionPath(query);

        if (isSelectByIdQuery(query)) {
            return documentQueryService.findDocumentById(query).getContent();
        } else {
            return selectByContent(query, collectionPath);
        }
    }




    private boolean isSelectByIdQuery(DocumentQuery query) {
        return query.getDocumentId() != null;
    }

    private List<JsonNode> selectByContent(DocumentQuery query, String collectionPath) throws Exception {

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

    private List<JsonNode> filterAllCollectionDocuments(JsonNode condition, String collectionPath) throws Exception {
        List<Document> documents = documentQueryService.readDocumentsByCollectionPath(collectionPath);
        return filterService.filterDocuments(documents, condition);
    }

    private List<JsonNode> filterDocumentsFromMostSelectiveIndex(String mostSelectiveIndexField, JsonNode condition, String collectionPath) throws Exception {
        String indexPath = PathConstructor.constructUserGeneratedIndexPath(collectionPath, mostSelectiveIndexField);
        Index mostSelectiveIndex = indexPersistenceManager.loadIndex(indexPath);
        Set<String> indexPointers = mostSelectiveIndex.getPointers( condition.get(mostSelectiveIndexField) );
        List<Document> documents = documentQueryService.readDocumentsByDocumentsPathList(indexPointers);

        return filterService.filterDocuments(documents, condition);
    }
}
