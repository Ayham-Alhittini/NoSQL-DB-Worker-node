package com.atypon.decentraldbcluster.test.builder;

import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.query.actions.DocumentAction;
import com.atypon.decentraldbcluster.query.types.DocumentQuery;
import com.fasterxml.jackson.databind.JsonNode;

public class DocumentQueryBuilder implements QueryBuilder {
    private final DocumentQuery query = new DocumentQuery();


    public DocumentQueryBuilder withId(String documentId) {
        query.setDocumentId(documentId);
        return this;
    }

    public DocumentQueryBuilder withCondition(JsonNode condition) {
        query.setCondition(condition);
        return this;
    }

    public DocumentQueryBuilder addDocument(JsonNode content) {
        query.setDocumentAction(DocumentAction.ADD);
        query.setContent(content);
        return this;
    }

    public DocumentQueryBuilder deleteDocument(String documentId) {
        query.setDocumentAction(DocumentAction.DELETE);
        query.setDocumentId(documentId);
        return this;
    }

    public DocumentQueryBuilder updateDocument(String documentId, JsonNode newContent) {
        query.setDocumentAction(DocumentAction.UPDATE);
        query.setDocumentId(documentId);
        query.setNewContent(newContent);
        return this;
    }

    public DocumentQueryBuilder replaceDocument(String documentId, JsonNode newContent) {
        query.setDocumentAction(DocumentAction.REPLACE);
        query.setDocumentId(documentId);
        query.setNewContent(newContent);
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
