package com.atypon.decentraldbcluster.query.databases;

import com.atypon.decentraldbcluster.query.base.Query;

public class DatabaseQuery extends Query {
    private DatabaseAction databaseAction;

    public DatabaseAction getDatabaseAction() {
        return databaseAction;
    }

    public void setDatabaseAction(DatabaseAction databaseAction) {
        this.databaseAction = databaseAction;
    }

    @Override
    public String toString() {
        return "DatabaseQuery{" +
                "action=" + databaseAction +
                ", originator='" + originator + '\'' +
                ", database='" + database + '\'' +
                ", collection='" + collection + '\'' +
                '}';
    }
}
