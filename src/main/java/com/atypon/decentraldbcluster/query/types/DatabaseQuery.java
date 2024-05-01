package com.atypon.decentraldbcluster.query.types;

import com.atypon.decentraldbcluster.query.actions.DatabaseAction;

public class DatabaseQuery extends Query {
    private DatabaseAction databaseAction;

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
}
