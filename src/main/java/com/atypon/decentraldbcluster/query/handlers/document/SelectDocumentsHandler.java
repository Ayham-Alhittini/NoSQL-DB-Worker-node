package com.atypon.decentraldbcluster.query.handlers.document;

import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.index.Index;
import com.atypon.decentraldbcluster.storage.managers.DocumentStorageManager;
import com.atypon.decentraldbcluster.storage.managers.IndexStorageManager;
import com.atypon.decentraldbcluster.query.types.DocumentQuery;
import com.atypon.decentraldbcluster.utility.IndexUtil;
import com.atypon.decentraldbcluster.utility.PathConstructor;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Service
public class SelectDocumentsHandler {
    private final IndexStorageManager indexStorageManager;
    private final DocumentStorageManager documentStorageManager;


    public SelectDocumentsHandler(IndexStorageManager indexPersistenceManager, DocumentStorageManager documentStorageManager) {
        this.indexStorageManager = indexPersistenceManager;
        this.documentStorageManager = documentStorageManager;
    }


    public Object handle(DocumentQuery query) throws Exception {

        String collectionPath = PathConstructor.constructCollectionPath(query);

        if (isSelectByIdQuery(query)) {
            String documentPath = PathConstructor.constructDocumentPath(query);
            return documentStorageManager.loadDocument(documentPath).getContent();
        } else {
            return selectByContent(query, collectionPath);
        }
    }


    private boolean isSelectByIdQuery(DocumentQuery query) {
        return query.getDocumentId() != null;
    }

    private List<JsonNode> selectByContent(DocumentQuery query, String collectionPath) throws Exception {

        String mostSelectiveIndexField = getMostSelectiveIndexFiled(query.getCondition(), collectionPath);

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
        List<Document> documents = documentStorageManager.getCollectionDocuments(collectionPath);
        return filterDocuments(documents, condition);
    }

    private List<JsonNode> filterDocumentsFromMostSelectiveIndex(String mostSelectiveIndexField, JsonNode condition, String collectionPath) throws Exception {
        String indexPath = PathConstructor.constructIndexPath(collectionPath, mostSelectiveIndexField);
        Index mostSelectiveIndex = indexStorageManager.loadIndex(indexPath);
        Set<String> indexPointers = mostSelectiveIndex.getPointers( condition.get(mostSelectiveIndexField) );
        List<Document> documents = readDocumentsByDocumentsPathList(indexPointers);

        return filterDocuments(documents, condition);
    }


    public List<Document> readDocumentsByDocumentsPathList(Set<String> documentsPath) throws Exception {
        List<Document> documents = new ArrayList<>();
        for (var documentPath: documentsPath) {
            documents.add( documentStorageManager.loadDocument(documentPath) );
        }
        return documents;
    }







    public List<JsonNode> filterDocuments(List<Document> documents, JsonNode filter) {

        List<JsonNode> filteredDocuments = new ArrayList<>();

        for (var document: documents) {
            if (isDocumentMatch(filter, document.getContent()))
                filteredDocuments.add(document.getContent());
        }

        return filteredDocuments;
    }

    private boolean isDocumentMatch(JsonNode filter, JsonNode documentData) {
        var fields = filter.fields();

        while (fields.hasNext()) {
            var field = fields.next();

            if (documentData.get(field.getKey()) == null || !documentData.get(field.getKey()).equals(field.getValue()))
                return false;
        }
        return true;
    }


    public String getMostSelectiveIndexFiled(JsonNode filter, String collectionPath) throws Exception {
        List<String> indexedFields = IndexUtil.getIndexedFields(filter, collectionPath);
        if (indexedFields.isEmpty()) return null; // No indexes found

        int minSelectiveSize = Integer.MAX_VALUE;
        String mostSelectiveIndex = null;

        for (String field: indexedFields) {
            var index = indexStorageManager.loadIndex( PathConstructor.constructIndexPath(collectionPath, field) );

            var pointers = index.getPointers( filter.get(field) );

            if (pointers.size() < minSelectiveSize) {
                minSelectiveSize = pointers.size();
                mostSelectiveIndex = field;
            }

            if (minSelectiveSize == 0) return field;// Can't reduce any longer
        }
        return mostSelectiveIndex;
    }
}
