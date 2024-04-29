package com.atypon.decentraldbcluster.query.builder;

import com.atypon.decentraldbcluster.query.actions.IndexAction;
import com.atypon.decentraldbcluster.query.types.IndexQuery;

public class IndexQueryBuilder implements QueryBuilder {
    private final IndexQuery query = new IndexQuery();

    public IndexQueryBuilder createIndex(String field) {
        query.setField(field);
        query.setIndexAction(IndexAction.CREATE);
        return this;
    }

    public IndexQueryBuilder dropIndex(String field) {
        query.setField(field);
        query.setIndexAction(IndexAction.DROP);
        return this;
    }

    @Override
    public IndexQueryBuilder withOriginator(String originator) {
        query.setOriginator(originator);
        return this;
    }

    @Override
    public IndexQueryBuilder withDatabase(String database) {
        query.setDatabase(database);
        return this;
    }

    @Override
    public IndexQueryBuilder withCollection(String collection) {
        query.setCollection(collection);
        return this;
    }

    @Override
    public IndexQuery build() {
        return query;
    }
}
