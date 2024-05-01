package com.atypon.decentraldbcluster.query.executors;

import com.atypon.decentraldbcluster.query.handlers.index.IndexHandler;
import com.atypon.decentraldbcluster.query.types.IndexQuery;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class IndexQueryExecutor implements Executable<IndexQuery> {

    private final IndexHandler indexHandler;
    @Autowired
    public IndexQueryExecutor(IndexHandler indexHandler) {
        this.indexHandler = indexHandler;
    }

    @Override
    public Object exec(IndexQuery query) throws Exception {
        return switch (query.getIndexAction()) {
            case CREATE -> indexHandler.handleCreateIndex(query);
            case DROP -> indexHandler.handleDropIndex(query);
            case SHOW_INDEXES -> indexHandler.handleShowIndexes(query);
        };
    }
}
