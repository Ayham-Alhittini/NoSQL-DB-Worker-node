package com.atypon.decentraldbcluster.query.databases;

import com.atypon.decentraldbcluster.query.base.Executable;
import com.atypon.decentraldbcluster.services.FileSystemService;
import com.atypon.decentraldbcluster.services.PathConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

@Component
public class DatabaseQueryExecutor implements Executable<DatabaseQuery> {

    private final FileSystemService fileSystemService;
    private final String rootDirectory = PathConstructor.getRootDirectory();

    @Autowired
    public DatabaseQueryExecutor(FileSystemService fileSystemService) {
        this.fileSystemService = fileSystemService;
    }


    @Override
    public Object exec(DatabaseQuery query) throws IOException {
        return switch (query.getDatabaseAction()) {
            case CREATE -> handleCreateDb(query);
            case DROP -> handleDropDb(query);
            case SHOW -> handleShowDbs(query);
        };
    }

    private Void handleCreateDb(DatabaseQuery query) throws IOException {
        String databasePath = Paths.get(rootDirectory, query.getOriginator(), query.getDatabase()).toString();
        fileSystemService.createDirectory(databasePath);
        return null;
    }

    private Void handleDropDb(DatabaseQuery query) throws IOException {
        String databasePath = Paths.get(rootDirectory, query.getOriginator(), query.getDatabase()).toString();
        fileSystemService.deleteDirectory(databasePath);
        return null;
    }

    private List<String> handleShowDbs(DatabaseQuery query)  {
        return fileSystemService.listAllDirectories(rootDirectory + "/" + query.getOriginator());
    }
}
