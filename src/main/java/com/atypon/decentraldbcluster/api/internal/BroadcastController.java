package com.atypon.decentraldbcluster.api.internal;

import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.services.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.nio.file.Paths;

@RestController
@RequestMapping("/internal/api/broadcast")
@CrossOrigin("*")
public class BroadcastController {

    private final UserDetails userDetails;
    private final FileSystemService fileSystemService;
    private final DocumentIndexService documentIndexService;
    private final DocumentService documentService;
    private final ObjectMapper mapper;

    @Autowired
    public BroadcastController(UserDetails userDetails, FileSystemService fileSystemService, DocumentIndexService documentIndexService, DocumentService documentService, ObjectMapper mapper) {
        this.userDetails = userDetails;
        this.fileSystemService = fileSystemService;
        this.documentIndexService = documentIndexService;
        this.documentService = documentService;
        this.mapper = mapper;
    }

    @PostMapping("/createDB/{database}")
    public void createDatabase(HttpServletRequest request, @PathVariable String database) throws IOException {

        String rootDirectory = PathConstructor.getRootDirectory();
        String userDirectory = userDetails.getUserDirectory(request);

        String databasePath = Paths.get(rootDirectory, userDirectory, database).toString();
        fileSystemService.createDirectory(databasePath);

    }

    @DeleteMapping("/dropDB/{database}")
    public void deleteDatabase(HttpServletRequest request, @PathVariable String database) throws IOException {

        String rootDirectory = PathConstructor.getRootDirectory();
        String userDirectory = userDetails.getUserDirectory(request);

        String databasePath = Paths.get(rootDirectory, userDirectory, database).toString();
        fileSystemService.deleteDirectory( databasePath);

    }

    @PostMapping("createCollection/{database}/{collection}")
    public void createCollection(HttpServletRequest request,
                                 @PathVariable String database,
                                 @PathVariable String collection,
                                 @RequestBody JsonNode schema) throws Exception {

        String userDirectory = userDetails.getUserDirectory(request);
        String collectionPath = PathConstructor.constructCollectionPath(userDirectory, database, collection);

        fileSystemService.createDirectory( Paths.get(collectionPath, "documents").toString() );
        fileSystemService.createDirectory( Paths.get(collectionPath, "indexes", "system_generated_indexes").toString() );
        fileSystemService.createDirectory( Paths.get(collectionPath, "indexes", "user_generated_indexes").toString() );

        fileSystemService.saveFile(schema.toPrettyString(), Paths.get(collectionPath, "schema.json").toString() );

        documentIndexService.createSystemIdIndex(collectionPath);
    }

    @DeleteMapping("dropCollection/{database}/{collection}")
    public void deleteCollection(HttpServletRequest request,
                                 @PathVariable String database,
                                 @PathVariable String collection) throws IOException {

        String userDirectory = userDetails.getUserDirectory(request);
        String collectionPath = PathConstructor.constructCollectionPath(userDirectory, database, collection);

        fileSystemService.deleteDirectory(collectionPath);

    }

    @PostMapping("addDocument/{database}/{collection}")
    public Document addDocument(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @RequestBody Document document) throws Exception {

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

        documentIndexService.deleteDocumentFromIndexes(documentPath);
        fileSystemService.deleteFile(documentPath);

    }

    //No optimistic locking
    @PutMapping("updateDocument/{database}/{collection}/{documentId}")
    public Document updateDocument(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @PathVariable String documentId, @RequestBody JsonNode requestBody) throws Exception {

        String userDirectory = userDetails.getUserDirectory(request);
        String collectionPath = PathConstructor.constructCollectionPath(userDirectory, database, collection);
        String documentPath = PathConstructor.constructDocumentPath(collectionPath, documentId);

        Document document = documentService.readDocument(documentPath);
        JsonNode updatedDocumentData = documentService.patchDocument(requestBody, document.getData());
        document.setData(updatedDocumentData);

        documentIndexService.updateIndexes(document, requestBody, collectionPath);
        fileSystemService.saveFile( mapper.valueToTree(document).toPrettyString() , documentPath);

        return document;
    }

    @PostMapping("createIndex/{database}/{collection}/{field}")
    public void createIndex(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @PathVariable String field) throws Exception {

        String userDirectory = userDetails.getUserDirectory(request);
        String collectionPath = PathConstructor.constructCollectionPath(userDirectory, database, collection);

        // TODO: handle field not exists
        documentIndexService.createIndex(collectionPath, field);
    }

    @DeleteMapping("dropIndex/{database}/{collection}/{field}")
    public void deleteIndex(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @PathVariable String field) throws Exception {

        String userDirectory = userDetails.getUserDirectory(request);
        String collectionPath = PathConstructor.constructCollectionPath(userDirectory, database, collection);
        String indexPath = PathConstructor.constructUserGeneratedIndexPath(collectionPath, field);

        fileSystemService.deleteFile(indexPath);
    }

}
