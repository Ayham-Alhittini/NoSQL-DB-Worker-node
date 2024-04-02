package com.atypon.decentraldbcluster.api.internal;

import com.atypon.decentraldbcluster.affinity.AffinityLoadBalancer;
import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.query.QueryExecutor;
import com.atypon.decentraldbcluster.query.base.Query;
import com.atypon.decentraldbcluster.query.collections.CollectionQueryBuilder;
import com.atypon.decentraldbcluster.query.databases.DatabaseQueryBuilder;
import com.atypon.decentraldbcluster.query.index.IndexQueryBuilder;
import com.atypon.decentraldbcluster.services.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/internal/api/broadcast")
@CrossOrigin("*")
public class BroadcastController {

    private final ObjectMapper mapper;
    private final UserDetails userDetails;
    private final QueryExecutor queryExecutor;
    private final DocumentReaderService documentService;
    private final FileSystemService fileSystemService;
    private final AffinityLoadBalancer affinityLoadBalancer;
    private final DocumentIndexService documentIndexService;


    @Autowired
    public BroadcastController(UserDetails userDetails, QueryExecutor queryExecutor, FileSystemService fileSystemService,
                               DocumentIndexService documentIndexService, DocumentReaderService documentService, ObjectMapper mapper,
                               AffinityLoadBalancer affinityLoadBalancer) {
        this.queryExecutor = queryExecutor;
        this.mapper = mapper;
        this.userDetails = userDetails;
        this.documentService = documentService;
        this.fileSystemService = fileSystemService;
        this.documentIndexService = documentIndexService;
        this.affinityLoadBalancer = affinityLoadBalancer;
    }

    //--------------- Database Broadcast

    @PostMapping("createDB/{database}")
    public void createDatabase(HttpServletRequest request, @PathVariable String database) throws Exception {

        DatabaseQueryBuilder builder = new DatabaseQueryBuilder();

        Query query = builder
                .withOriginator( userDetails.getUserId(request) )
                .createDatabase(database)
                .build();

        queryExecutor.exec(query);

    }

    @DeleteMapping("dropDB/{database}")
    public void deleteDatabase(HttpServletRequest request, @PathVariable String database) throws Exception {

        DatabaseQueryBuilder builder = new DatabaseQueryBuilder();

        Query query = builder
                .withOriginator( userDetails.getUserId(request) )
                .dropDatabase(database)
                .build();

        queryExecutor.exec(query);

    }

    //--------------- Collection Broadcast
    @PostMapping("createCollection/{database}/{collection}")
    public void createCollection(HttpServletRequest request,
                                 @PathVariable String database,
                                 @PathVariable String collection,
                                 @RequestBody JsonNode schema) throws Exception {

        CollectionQueryBuilder builder = new CollectionQueryBuilder();

        Query query = builder.withOriginator( userDetails.getUserId(request) )
                .withDatabase(database)
                .createCollection(collection)
                .withSchema(schema)
                .build();

        queryExecutor.exec(query);
    }

    @DeleteMapping("dropCollection/{database}/{collection}")
    public void deleteCollection(HttpServletRequest request,
                                 @PathVariable String database,
                                 @PathVariable String collection) throws Exception {

        CollectionQueryBuilder builder = new CollectionQueryBuilder();

        Query query = builder.withOriginator(userDetails.getUserId(request))
                .withDatabase(database)
                .dropCollection(collection)
                .build();

        queryExecutor.exec(query);
    }

    //--------------- Document Broadcast

    @PostMapping("addDocument/{database}/{collection}")
    public Document addDocument(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @RequestBody Document document) throws Exception {

        affinityLoadBalancer.incrementNodeAssignedDocuments(document.getAffinityPort());

        String userDirectory = userDetails.getUserDirectory(request);
        String collectionPath = PathConstructor.constructCollectionPath(userDirectory, database, collection);
        String documentPath = PathConstructor.constructDocumentPath(collectionPath, document.getId());

        fileSystemService.saveFile( mapper.valueToTree(document).toPrettyString() , documentPath);
        documentIndexService.insertToAllIndexes(document, documentPath);
        return document;
    }

    @DeleteMapping("deleteDocument/{database}/{collection}/{documentId}")
    public void deleteDocument(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @PathVariable String documentId) throws Exception {

        String userDirectory = userDetails.getUserDirectory(request);
        String collectionPath = PathConstructor.constructCollectionPath(userDirectory, database, collection);
        String documentPath = PathConstructor.constructDocumentPath(collectionPath, documentId);

        Document document = documentService.readDocument(documentPath);
        affinityLoadBalancer.decrementNodeAssignedDocuments(document.getAffinityPort());

        documentIndexService.deleteDocumentFromIndexes(documentPath);
        fileSystemService.deleteFile(documentPath);

    }

    @PutMapping("updateDocument/{database}/{collection}/{documentId}")
    public Document updateDocument(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @PathVariable String documentId, @RequestBody JsonNode requestBody) throws Exception {

        String userDirectory = userDetails.getUserDirectory(request);
        String collectionPath = PathConstructor.constructCollectionPath(userDirectory, database, collection);
        String documentPath = PathConstructor.constructDocumentPath(collectionPath, documentId);

        Document document = documentService.readDocument(documentPath);
        document.incrementVersion();
        JsonNode updatedDocumentData = documentService.patchDocument(requestBody, document.getData());
        document.setData(updatedDocumentData);

        documentIndexService.updateIndexes(document, requestBody, collectionPath);
        fileSystemService.saveFile( mapper.valueToTree(document).toPrettyString() , documentPath);

        return document;
    }


    //--------------- Index Broadcast

    @PostMapping("createIndex/{database}/{collection}/{field}")
    public void createIndex(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @PathVariable String field) throws Exception {

        IndexQueryBuilder builder = new IndexQueryBuilder();

        Query query = builder.withOriginator( userDetails.getUserId(request) )
                .withDatabase(database)
                .withCollection(collection)
                .createIndex(field)
                .build();

        queryExecutor.exec(query);
    }

    @DeleteMapping("dropIndex/{database}/{collection}/{field}")
    public void deleteIndex(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @PathVariable String field) throws Exception {

        IndexQueryBuilder builder = new IndexQueryBuilder();

        Query query = builder.withOriginator( userDetails.getUserId(request) )
                .withDatabase(database)
                .withCollection(collection)
                .dropIndex(field)
                .build();

        queryExecutor.exec(query);
    }

}
