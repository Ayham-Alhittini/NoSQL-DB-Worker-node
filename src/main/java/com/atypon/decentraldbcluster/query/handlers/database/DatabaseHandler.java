package com.atypon.decentraldbcluster.query.handlers.database;

import com.atypon.decentraldbcluster.storage.disk.FileSystemService;
import com.atypon.decentraldbcluster.query.types.DatabaseQuery;
import com.atypon.decentraldbcluster.utility.PathConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Paths;
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
        String databasePath = Paths.get(rootDirectory, query.getOriginator(), query.getDatabase()).toString();
        fileSystemService.createDirectory(databasePath);
        return null;
    }

    public Void handleDropDb(DatabaseQuery query) throws IOException {
        String databasePath = Paths.get(rootDirectory, query.getOriginator(), query.getDatabase()).toString();
        fileSystemService.deleteDirectory(databasePath);
        return null;
    }

    public List<String> handleShowDbs(DatabaseQuery query)  {
        return fileSystemService.getAllDirectories(rootDirectory + "/" + query.getOriginator());
    }
}
