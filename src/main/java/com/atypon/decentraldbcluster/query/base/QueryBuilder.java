package com.atypon.decentraldbcluster.query.base;

public interface QueryBuilder {
    QueryBuilder withOriginator(String originator);
    QueryBuilder withDatabase(String database);
    QueryBuilder withCollection(String collection);
    Query build();
}
