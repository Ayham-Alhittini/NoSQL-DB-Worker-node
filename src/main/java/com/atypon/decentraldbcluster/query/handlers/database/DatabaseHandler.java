package com.atypon.decentraldbcluster.query.handlers.database;

import com.atypon.decentraldbcluster.entity.Database;
import com.atypon.decentraldbcluster.storage.disk.FileSystemService;
import com.atypon.decentraldbcluster.query.types.DatabaseQuery;
import com.atypon.decentraldbcluster.utility.PathConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@Service
public class DatabaseHandler {
    private final FileSystemService fileSystemService;
    private final String rootDirectory = PathConstructor.getRootDirectory();

    @Autowired
    public DatabaseHandler(FileSystemService fileSystemService) {
        this.fileSystemService = fileSystemService;
    }

    public Void handleCreateDb(DatabaseQuery query) throws IOException {
        String databasePath = Paths.get(rootDirectory, query.getOriginator(), query.getDatabaseName()).toString();
        fileSystemService.createDirectory(databasePath);
        fileSystemService.saveFile(query.getDatabaseConnection(), databasePath + "/connection.txt");
        return null;
    }

    public Void handleDropDb(DatabaseQuery query) throws IOException {
        String databasePath = Paths.get(rootDirectory, query.getOriginator(), query.getDatabaseName()).toString();
        fileSystemService.deleteDirectory(databasePath);
        return null;
    }

    public List<Database> handleShowDbs(DatabaseQuery query)  {
        List<String> databasesName = fileSystemService.getAllDirectories(rootDirectory + "/" + query.getOriginator());
        List<Database> databases = new ArrayList<>();

        for (String dbName: databasesName) {
            String apiKeyPath = Path.of(rootDirectory, query.getOriginator(), dbName, "connection.txt").toString();
            String dbApiKey = fileSystemService.loadFileContent(apiKeyPath);
            databases.add(new Database(dbName, dbApiKey));
        }

        return databases;
    }
}
