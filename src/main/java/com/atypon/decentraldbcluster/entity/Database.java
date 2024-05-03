package com.atypon.decentraldbcluster.entity;

public class Database {
    private String dbName;
    private String apiKey;

    public Database(){}

    public Database(String dbName, String apiKey) {
        this.dbName = dbName;
        this.apiKey = apiKey;
    }

    public String getDbName() {
        return dbName;
    }

    public String getApiKey() {
        return apiKey;
    }
}
