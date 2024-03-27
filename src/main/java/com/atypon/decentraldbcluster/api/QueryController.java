package com.atypon.decentraldbcluster.api;

import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.index.Index;
import com.atypon.decentraldbcluster.services.*;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Set;

@RestController
@RequestMapping("/api/query")
@CrossOrigin("*")
public class QueryController {

    private final IndexService indexService;
    private final QueryService queryService;
    private final UserDetails userDetails;
    private final DocumentService documentService;

    @Autowired
    public QueryController(IndexService indexService, UserDetails userDetails, QueryService queryService, DocumentService documentService) {
        this.indexService = indexService;
        this.userDetails = userDetails;
        this.queryService = queryService;
        this.documentService = documentService;
    }

    @GetMapping("{database}/{collection}/findOne/{documentId}")
    public Document getData(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @PathVariable String documentId) throws Exception {

        String userDirectory = userDetails.getUserDirectory(request);
        String collectionPath = FileStorageService.constructCollectionPath(userDirectory, database, collection);

        return queryService.findDocumentById(collectionPath, documentId);
    }


    @GetMapping("{database}/{collection}/find")
    public List<Document> find(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @RequestBody JsonNode filter) throws Exception {

        String userDirectory = userDetails.getUserDirectory(request);
        String collectionPath = FileStorageService.constructCollectionPath(userDirectory, database, collection);

        String mostSelectiveIndexField = queryService.getMostSelectiveIndexFiled(filter, collectionPath);

        if (mostSelectiveIndexField == null) {
            List<Document> documents = documentService.readDocumentsByCollectionPath(collectionPath);
            return queryService.filterDocuments(documents, filter);
        }

        String indexPath = indexService.constructUserGeneratedIndexPath(collectionPath, mostSelectiveIndexField);
        Index mostSelectiveIndex = indexService.loadIndex(indexPath);

        Set<String> indexPointers = mostSelectiveIndex.getPointers( filter.get(mostSelectiveIndexField) );

        List<Document> documents = documentService.readDocumentsByDocumentsPathList(indexPointers);

        return queryService.filterDocuments(documents, filter);
    }

}
