package com.atypon.decentraldbcluster.query.types;

//Implementation for each query type at the query executor
public abstract class Query {
    protected String originator;
    protected String database;
    protected String collection;

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
}
