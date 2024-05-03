package com.atypon.decentraldbcluster.query.types;

import com.atypon.decentraldbcluster.query.actions.DatabaseAction;

public class DatabaseQuery extends Query {
    private DatabaseAction databaseAction;
    private String databaseConnection;

    public DatabaseAction getDatabaseAction() {
        return databaseAction;
    }

    public void setDatabaseAction(DatabaseAction databaseAction) {
        this.databaseAction = databaseAction;
    }

    @Override
    public boolean isWriteQuery() {
        return databaseAction != DatabaseAction.SHOW;
    }

    public String getDatabaseConnection() {
        return databaseConnection;
    }

    public void setDatabaseConnection(String databaseConnection) {
        this.databaseConnection = databaseConnection;
    }
}
