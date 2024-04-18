package com.atypon.decentraldbcluster.query.types;

import com.atypon.decentraldbcluster.query.actions.CollectionAction;
import com.fasterxml.jackson.databind.JsonNode;

public class CollectionQuery extends Query {
    private CollectionAction collectionAction;
    private JsonNode schema;

    public CollectionAction getCollectionAction() {
        return collectionAction;
    }

    public void setCollectionAction(CollectionAction collectionAction) {
        this.collectionAction = collectionAction;
    }

    public JsonNode getSchema() {
        return schema;
    }

    public void setSchema(JsonNode schema) {
        this.schema = schema;
    }
}
