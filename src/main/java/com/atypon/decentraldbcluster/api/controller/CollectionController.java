package com.atypon.decentraldbcluster.api.controller;

import com.atypon.decentraldbcluster.services.FileStorageService;
import com.atypon.decentraldbcluster.services.IndexService;
import com.atypon.decentraldbcluster.services.UserDetails;
import com.atypon.decentraldbcluster.services.JsonSchemaValidator;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

@RestController
@RequestMapping("/api/collection")
@CrossOrigin("*")
public class CollectionController {

    private final UserDetails userDetails;
    private final JsonSchemaValidator schemaValidator;
    private final IndexService indexService;

    @Autowired
    public CollectionController(UserDetails userDetails, JsonSchemaValidator schemaValidator, IndexService indexService) {
        this.userDetails = userDetails;
        this.schemaValidator = schemaValidator;
        this.indexService = indexService;
    }

    @PostMapping("{database}/create/{collection}")
    public void createCollection(HttpServletRequest request,
                                              @PathVariable String database,
                                              @PathVariable String collection,
                                              @RequestBody JsonNode schema) throws Exception {

        schemaValidator.validateSchemaDataTypes(schema);


        String userDirectory = userDetails.getUserDirectory(request);
        String collectionPath = FileStorageService.constructCollectionPath(userDirectory, database, collection);

        FileStorageService.createDirectory( Paths.get(collectionPath, "documents").toString() );
        FileStorageService.createDirectory( Paths.get(collectionPath, "indexes").toString() );

        FileStorageService.saveFile(schema.toPrettyString(), Paths.get(collectionPath, "schema.json").toString() );

        // Auto creation for _id field
        indexService.createIndex(collectionPath, "_id");
    }



    @DeleteMapping("{database}/delete/{collection}")
    public void deleteCollection(HttpServletRequest request,
                                              @PathVariable String database,
                                              @PathVariable String collection) throws IOException {

        String userDirectory = userDetails.getUserDirectory(request);

        String collectionPath = FileStorageService.constructCollectionPath(userDirectory, database, collection);
        FileStorageService.deleteDirectory(collectionPath);

    }




    @GetMapping("{database}/showCollections")
    public ResponseEntity<List<String>> showCollections(HttpServletRequest request, @PathVariable String database) {

        String rootDirectory = FileStorageService.getRootDirectory();
        String userDirectory = userDetails.getUserId(request);

        String databasePath = Paths.get(rootDirectory, userDirectory, database).toString();

        List<String> dbs = FileStorageService.listAllDirectories(databasePath);

        return ResponseEntity.ok(dbs);
    }
}
