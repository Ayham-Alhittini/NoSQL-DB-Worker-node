package com.atypon.decentraldbcluster.query.builder;

import com.atypon.decentraldbcluster.query.types.Query;

public interface QueryBuilder {
    QueryBuilder withOriginator(String originator);
    QueryBuilder withDatabase(String database);
    QueryBuilder withCollection(String collection);
    Query build();
}
