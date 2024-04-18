package com.atypon.decentraldbcluster.query.types;

import com.atypon.decentraldbcluster.query.actions.IndexAction;

public class IndexQuery extends Query {
    private IndexAction indexAction;
    private String field;


    public IndexAction getIndexAction() {
        return indexAction;
    }

    public void setIndexAction(IndexAction indexAction) {
        this.indexAction = indexAction;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }
}
