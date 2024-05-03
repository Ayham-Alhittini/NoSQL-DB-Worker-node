package com.atypon.decentraldbcluster.query.handlers.collection;

import com.atypon.decentraldbcluster.storage.disk.FileSystemService;
import com.atypon.decentraldbcluster.query.types.CollectionQuery;
import com.atypon.decentraldbcluster.utility.PathConstructor;
import com.atypon.decentraldbcluster.schema.SchemaCreator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

@Service
public class CollectionHandler {
    private final SchemaCreator schemaCreator;
    private final FileSystemService fileSystemService;

    @Autowired
    public CollectionHandler(SchemaCreator schemaCreator, FileSystemService fileSystemService) {
        this.schemaCreator = schemaCreator;
        this.fileSystemService = fileSystemService;
    }


    public Void handleDropCollection(CollectionQuery query) throws IOException {
        String collectionPath = PathConstructor.constructCollectionPath(query.getOriginator(), query.getDatabaseName(), query.getCollection());
        fileSystemService.deleteDirectory(collectionPath);
        return null;
    }


    public List<String> handleShowCollections(CollectionQuery query) {
        String rootDirectory = PathConstructor.getRootDirectory();
        String databasePath = Paths.get(rootDirectory, query.getOriginator(), query.getDatabaseName()).toString();
        return fileSystemService.getAllDirectories(databasePath);
    }

    public String handleShowSchema(CollectionQuery query) {
        String collectionPath = PathConstructor.constructCollectionPath(query);
        String schemaPath = Path.of(collectionPath, "schema.json").toString();

        if (fileSystemService.isFileExists(schemaPath))
            return fileSystemService.loadFileContent(collectionPath + "/schema.json");
        return null;
    }


    public Void handleCreateCollection(CollectionQuery query) throws Exception {

        String collectionPath = PathConstructor.constructCollectionPath(query.getOriginator(), query.getDatabaseName(), query.getCollection());

        fileSystemService.createDirectory(Paths.get(collectionPath, "documents").toString() );
        fileSystemService.createDirectory( Paths.get(collectionPath, "indexes").toString() );

        if (query.getSchema() != null && !query.getSchema().isNull()) {
            schemaCreator.validateAndCreateSchema(query.getSchema(), collectionPath);
        }

        return null;
    }
}
