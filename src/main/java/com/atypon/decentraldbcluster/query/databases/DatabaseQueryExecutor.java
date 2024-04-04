package com.atypon.decentraldbcluster.query.databases;

import com.atypon.decentraldbcluster.query.base.Executable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class DatabaseQueryExecutor implements Executable<DatabaseQuery> {

    private final DatabaseHandler databaseHandler;

    @Autowired
    public DatabaseQueryExecutor(DatabaseHandler databaseHandler) {
        this.databaseHandler = databaseHandler;
    }


    @Override
    public Object exec(DatabaseQuery query) throws IOException {
        return switch (query.getDatabaseAction()) {
            case CREATE -> databaseHandler.handleCreateDb(query);
            case DROP -> databaseHandler.handleDropDb(query);
            case SHOW -> databaseHandler.handleShowDbs(query);
        };
    }
}
