package com.atypon.decentraldbcluster.query.collections;

import com.atypon.decentraldbcluster.query.base.QueryBuilder;

public class CollectionQueryBuilder implements QueryBuilder {

    private final CollectionQuery query = new CollectionQuery();

    public CollectionQueryBuilder createCollection(String collection) {
        query.setCollection(collection);
        query.setCollectionAction(CollectionAction.CREATE);
        return this;
    }

    public CollectionQueryBuilder dropCollection(String collection) {
        query.setCollection(collection);
        query.setCollectionAction(CollectionAction.DROP);
        return this;
    }

    public CollectionQueryBuilder showCollections() {
        query.setCollectionAction(CollectionAction.SHOW);
        return this;
    }

    @Override
    public CollectionQueryBuilder withOriginator(String originator) {
        query.setOriginator(originator);
        return this;
    }

    @Override
    public CollectionQueryBuilder withDatabase(String database) {
        query.setDatabase(database);
        return this;
    }

    @Override
    public CollectionQueryBuilder withCollection(String collection) {
        throw new UnsupportedOperationException("Use add, remove collection for collection query");
    }

    @Override
    public CollectionQuery build() {
        return query;
    }
}
