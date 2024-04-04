package com.atypon.decentraldbcluster.query.collections;

import com.atypon.decentraldbcluster.services.DocumentIndexService;
import com.atypon.decentraldbcluster.services.FileSystemService;
import com.atypon.decentraldbcluster.utility.PathConstructor;
import com.atypon.decentraldbcluster.validation.SchemaValidator;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

@Service
public class CollectionHandler {
    private final SchemaValidator schemaValidator;
    private final FileSystemService fileSystemService;
    private final DocumentIndexService documentIndexService;

    @Autowired
    public CollectionHandler(SchemaValidator schemaValidator, FileSystemService fileSystemService, DocumentIndexService documentIndexService) {
        this.schemaValidator = schemaValidator;
        this.fileSystemService = fileSystemService;
        this.documentIndexService = documentIndexService;
    }


    public Void handleDropCollection(CollectionQuery query) throws IOException {
        String collectionPath = PathConstructor.constructCollectionPath(query.getOriginator(), query.getDatabase(), query.getCollection());
        fileSystemService.deleteDirectory(collectionPath);
        return null;
    }


    public List<String> handleShowCollections(CollectionQuery query) {
        String rootDirectory = PathConstructor.getRootDirectory();
        String databasePath = Paths.get(rootDirectory, query.getOriginator(), query.getDatabase()).toString();
        return fileSystemService.listAllDirectories(databasePath);
    }


    private void saveSchemaIfExists(JsonNode schema, String collectionPath) throws IOException {
        if (schema != null && !schema.isNull())
            fileSystemService.saveFile(schema.toPrettyString(), Paths.get(collectionPath, "schema.json").toString() );
    }


    public Void handleCreateCollection(CollectionQuery query) throws Exception {

        schemaValidator.validateSchemaDataTypesIfExists(query.getSchema());
        String collectionPath = PathConstructor.constructCollectionPath(query.getOriginator(), query.getDatabase(), query.getCollection());

        fileSystemService.createDirectory(Paths.get(collectionPath, "documents").toString() );
        fileSystemService.createDirectory( Paths.get(collectionPath, "indexes", "system_generated_indexes").toString() );
        fileSystemService.createDirectory( Paths.get(collectionPath, "indexes", "user_generated_indexes").toString() );

        saveSchemaIfExists(query.getSchema(), collectionPath);

        documentIndexService.createSystemIdIndex(collectionPath);
        return null;
    }
}
