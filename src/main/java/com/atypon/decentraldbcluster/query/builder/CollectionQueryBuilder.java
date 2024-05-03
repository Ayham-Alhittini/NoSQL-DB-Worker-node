package com.atypon.decentraldbcluster.query.builder;

import com.atypon.decentraldbcluster.query.actions.CollectionAction;
import com.atypon.decentraldbcluster.query.types.CollectionQuery;
import com.fasterxml.jackson.databind.JsonNode;

public class CollectionQueryBuilder implements QueryBuilder {

    private final CollectionQuery query = new CollectionQuery();

    public CollectionQueryBuilder createCollection(String collection) {
        query.setCollection(collection);
        query.setCollectionAction(CollectionAction.CREATE);
        return this;
    }

    public CollectionQueryBuilder withSchema(JsonNode schema) {
        query.setSchema(schema);
        return this;
    }

    public CollectionQueryBuilder dropCollection(String collection) {
        query.setCollection(collection);
        query.setCollectionAction(CollectionAction.DROP);
        return this;
    }

    public CollectionQueryBuilder showCollections() {
        query.setCollectionAction(CollectionAction.SHOW_COLLECTIONS);
        return this;
    }

    public CollectionQueryBuilder showSchema() {
        query.setCollectionAction(CollectionAction.SHOW_SCHEMA);
        return this;
    }

    @Override
    public CollectionQueryBuilder withOriginator(String originator) {
        query.setOriginator(originator);
        return this;
    }

    @Override
    public CollectionQueryBuilder withDatabase(String database) {
        query.setDatabaseName(database);
        return this;
    }

    @Override
    public CollectionQueryBuilder withCollection(String collection) {
        query.setCollection(collection);
        return this;
    }

    @Override
    public CollectionQuery build() {
        return query;
    }
}
