package com.atypon.decentraldbcluster.api.controller;

import com.atypon.decentraldbcluster.services.FileStorageService;
import com.atypon.decentraldbcluster.services.UserDetails;
import com.atypon.decentraldbcluster.services.JsonSchemaValidator;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;

@RestController
@RequestMapping("/api/collection")
@CrossOrigin("*")
public class CollectionController {

    private final UserDetails userDetails;
    private final JsonSchemaValidator schemaValidator;

    @Autowired
    public CollectionController(UserDetails userDetails, JsonSchemaValidator schemaValidator) {
        this.userDetails = userDetails;
        this.schemaValidator = schemaValidator;
    }

    @PostMapping("{databaseName}/create/{collectionName}")
    public void createCollection(HttpServletRequest request,
                                              @PathVariable String databaseName ,
                                              @PathVariable String collectionName,
                                              @RequestBody JsonNode schema) throws IOException {

        schemaValidator.validateSchema(schema);

        String userDirectory = userDetails.getUserId(request);
        String collectionPath = userDirectory + "/" + databaseName + "/" + collectionName;

        FileStorageService.createDirectory(collectionPath + "/documents");
        FileStorageService.createDirectory(collectionPath + "/indexes");

        FileStorageService.saveFile(schema.toPrettyString(), collectionPath + "/schema.json");

    }



    @DeleteMapping("{databaseName}/delete/{collectionName}")
    public void deleteCollection(HttpServletRequest request,
                                              @PathVariable String databaseName ,
                                              @PathVariable String collectionName) throws IOException {

        String userDirectory = userDetails.getUserId(request);
        FileStorageService.deleteDirectory(userDirectory + "/" + databaseName + "/" + collectionName);

    }




    @GetMapping("{databaseName}/showCollections")
    public ResponseEntity<List<String>> showCollections(HttpServletRequest request, @PathVariable String databaseName) {

        String userDirectory = userDetails.getUserId(request);
        List<String> dbs = FileStorageService.listAllDirectories(userDirectory + "/" + databaseName);

        return ResponseEntity.ok(dbs);
    }
}
