package com.atypon.decentraldbcluster.api.external;

import com.atypon.decentraldbcluster.services.*;
import com.atypon.decentraldbcluster.validation.SchemaValidator;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpMethod;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

@RestController
@RequestMapping("/api/collection")
@CrossOrigin("*")
public class CollectionController {

    private final UserDetails userDetails;
    private final SchemaValidator schemaValidator;
    private final DocumentIndexService documentIndexService;
    private final FileSystemService fileSystemService;

    @Autowired
    public CollectionController(UserDetails userDetails, SchemaValidator schemaValidator,
                                DocumentIndexService documentIndexService, FileSystemService fileSystemService) {
        this.userDetails = userDetails;
        this.schemaValidator = schemaValidator;
        this.documentIndexService = documentIndexService;
        this.fileSystemService = fileSystemService;
    }

    //TODO: create schema less collection
    @PostMapping("{database}/create/{collection}")
    public void createCollection(HttpServletRequest request,
                                              @PathVariable String database,
                                              @PathVariable String collection,
                                              @RequestBody JsonNode schema) throws Exception {

        schemaValidator.validateSchemaDataTypes(schema);

        String userDirectory = userDetails.getUserDirectory(request);
        String collectionPath = PathConstructor.constructCollectionPath(userDirectory, database, collection);

        fileSystemService.createDirectory( Paths.get(collectionPath, "documents").toString() );
        fileSystemService.createDirectory( Paths.get(collectionPath, "indexes", "system_generated_indexes").toString() );
        fileSystemService.createDirectory( Paths.get(collectionPath, "indexes", "user_generated_indexes").toString() );

        fileSystemService.saveFile(schema.toPrettyString(), Paths.get(collectionPath, "schema.json").toString() );

        documentIndexService.createSystemIdIndex(collectionPath);

        BroadcastService.doBroadcast(request, "createCollection/" + database + "/" + collection, schema, HttpMethod.POST);

    }

    @DeleteMapping("{database}/delete/{collection}")
    public void deleteCollection(HttpServletRequest request,
                                              @PathVariable String database,
                                              @PathVariable String collection) throws IOException {

        String userDirectory = userDetails.getUserDirectory(request);

        String collectionPath = PathConstructor.constructCollectionPath(userDirectory, database, collection);
        fileSystemService.deleteDirectory(collectionPath);
        BroadcastService.doBroadcast(request, "dropCollection/" + database + "/" + collection, null, HttpMethod.DELETE);
    }

    @GetMapping("{database}/showCollections")
    public List<String> showCollections(HttpServletRequest request, @PathVariable String database) {

        String rootDirectory = PathConstructor.getRootDirectory();
        String userDirectory = userDetails.getUserId(request);

        String databasePath = Paths.get(rootDirectory, userDirectory, database).toString();

        return fileSystemService.listAllDirectories(databasePath);
    }
}
