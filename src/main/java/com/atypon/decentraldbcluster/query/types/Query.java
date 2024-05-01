package com.atypon.decentraldbcluster.query.types;

//Implementation for each query type at the query handlers
public abstract class Query {
    private String originator;
    private String database;
    private String collection;
    private boolean broadcastQuery;

    public String getOriginator() {
        return originator;
    }

    public void setOriginator(String originator) {
        this.originator = originator;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
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
