package com.atypon.decentraldbcluster.query.documents;

import com.atypon.decentraldbcluster.query.base.QueryBuilder;

public class DocumentQueryBuilder implements QueryBuilder {
    private final DocumentQuery query = new DocumentQuery();


    public DocumentQueryBuilder withCondition(String condition) {
        query.setCondition(condition);
        return this;
    }

    public DocumentQueryBuilder addDocument(String json) {
        query.setDocumentAction(DocumentAction.ADD);
        query.setContent(json);
        return this;
    }

    //Rely on condition
    public DocumentQueryBuilder deleteDocuments() {
        query.setDocumentAction(DocumentAction.DELETE);
        return this;
    }

    //Rely on condition
    public DocumentQueryBuilder updateDocuments(String json) {
        query.setDocumentAction(DocumentAction.UPDATE);
        query.setNewContent(json);
        return this;
    }

    //Rely on condition
    public DocumentQueryBuilder selectDocuments() {
        query.setDocumentAction(DocumentAction.SELECT);
        return this;
    }

    @Override
    public DocumentQueryBuilder withOriginator(String originator) {
        query.setOriginator(originator);
        return this;
    }

    @Override
    public DocumentQueryBuilder withDatabase(String database) {
        query.setDatabase(database);
        return this;
    }

    @Override
    public DocumentQueryBuilder withCollection(String collection) {
        query.setCollection(collection);
        return this;
    }

    @Override
    public DocumentQuery build() {
        return query;
    }
}
