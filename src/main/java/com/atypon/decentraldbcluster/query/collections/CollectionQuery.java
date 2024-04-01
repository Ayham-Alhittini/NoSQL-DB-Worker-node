package com.atypon.decentraldbcluster.query.collections;

import com.atypon.decentraldbcluster.query.base.Query;

public class CollectionQuery extends Query {
    private CollectionAction collectionAction;

    public CollectionAction getCollectionAction() {
        return collectionAction;
    }

    public void setCollectionAction(CollectionAction collectionAction) {
        this.collectionAction = collectionAction;
    }

    @Override
    public String toString() {
        return "CollectionQuery{" +
                "collectionAction=" + collectionAction +
                ", originator='" + originator + '\'' +
                ", database='" + database + '\'' +
                ", collection='" + collection + '\'' +
                '}';
    }
}
