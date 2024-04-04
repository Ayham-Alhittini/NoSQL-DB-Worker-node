package com.atypon.decentraldbcluster.query.documents;

import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.query.base.QueryBuilder;
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

    //Rely on condition
    public DocumentQueryBuilder deleteDocument(Document document) {
        query.setDocumentAction(DocumentAction.DELETE);
        query.setDocument(document);
        return this;
    }

    public DocumentQueryBuilder updateDocument(Document document, JsonNode newContent) {
        query.setDocumentAction(DocumentAction.UPDATE);
        query.setDocument(document);
        query.setNewContent(newContent);
        return this;
    }

    public DocumentQueryBuilder replaceDocument(Document document, JsonNode newContent) {
        query.setDocumentAction(DocumentAction.REPLACE);
        query.setDocument(document);
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
