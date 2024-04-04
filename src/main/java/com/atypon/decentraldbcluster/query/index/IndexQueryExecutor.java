package com.atypon.decentraldbcluster.query.index;

import com.atypon.decentraldbcluster.query.base.Executable;
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
        };
    }
}
