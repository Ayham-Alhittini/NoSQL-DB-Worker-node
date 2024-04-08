package com.atypon.decentraldbcluster.query.handlers.document;

import com.atypon.decentraldbcluster.disk.FileSystemService;
import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.index.Index;
import com.atypon.decentraldbcluster.query.types.DocumentQuery;
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
    private final FileSystemService fileSystemService;
    private final DocumentFilterService filterService;
    private final DocumentReaderService documentReaderService;

    public SelectDocumentsHandler(DocumentFilterService filterService, DocumentReaderService documentReaderService, FileSystemService fileSystemService) {
        this.filterService = filterService;
        this.documentReaderService = documentReaderService;
        this.fileSystemService = fileSystemService;
    }


    public Object handle(DocumentQuery query) throws Exception {

        String collectionPath = PathConstructor.constructCollectionPath(query);

        if (isSelectByIdQuery(query)) {
            return documentReaderService.findDocumentById(query);
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
        Index mostSelectiveIndex = fileSystemService.loadIndex(indexPath);
        Set<String> indexPointers = mostSelectiveIndex.getPointers( condition.get(mostSelectiveIndexField) );
        List<Document> documents = documentReaderService.readDocumentsByDocumentsPathList(indexPointers);

        return filterService.filterDocuments(documents, condition);
    }
}
