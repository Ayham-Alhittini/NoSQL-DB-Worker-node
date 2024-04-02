package com.atypon.decentraldbcluster.query.collections;

import com.atypon.decentraldbcluster.query.base.Query;
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

    @Override
    public String toString() {
        return "CollectionQuery{" +
                "collectionAction=" + collectionAction +
                ", schema='" + schema + '\'' +
                ", originator='" + originator + '\'' +
                ", database='" + database + '\'' +
                ", collection='" + collection + '\'' +
                '}';
    }
}
