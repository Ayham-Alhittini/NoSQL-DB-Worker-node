package com.atypon.decentraldbcluster.query.executors;

import com.atypon.decentraldbcluster.query.handlers.collection.CollectionHandler;
import com.atypon.decentraldbcluster.query.types.CollectionQuery;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class CollectionQueryExecutor implements Executable<CollectionQuery> {

    private final CollectionHandler collectionHandler;

    @Autowired
    public CollectionQueryExecutor(CollectionHandler collectionHandler) {
        this.collectionHandler = collectionHandler;
    }

    @Override
    public Object exec(CollectionQuery query) throws Exception {
        return switch (query.getCollectionAction()) {
            case CREATE -> collectionHandler.handleCreateCollection(query);
            case DROP -> collectionHandler.handleDropCollection(query);
            case SHOW -> collectionHandler.handleShowCollections(query);
        };
    }
}
