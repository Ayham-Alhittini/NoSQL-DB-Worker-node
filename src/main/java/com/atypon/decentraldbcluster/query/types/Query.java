package com.atypon.decentraldbcluster.query.types;

//Implementation for each query type at the query handlers
public abstract class Query {
    private String originator;
    private String databaseName;
    private String collection;
    private boolean broadcastQuery;

    public String getOriginator() {
        return originator;
    }

    public void setOriginator(String originator) {
        this.originator = originator;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public boolean isBroadcastQuery() {
        return broadcastQuery;
    }

    public void setBroadcastQuery(boolean broadcastQuery) {
        this.broadcastQuery = broadcastQuery;
    }
    public abstract boolean isWriteQuery();
}
