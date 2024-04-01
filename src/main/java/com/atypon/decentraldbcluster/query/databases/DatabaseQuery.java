package com.atypon.decentraldbcluster.query.databases;

import com.atypon.decentraldbcluster.query.base.Query;

public class DatabaseQuery extends Query {
    private DatabaseAction action;

    public DatabaseAction getAction() {
        return action;
    }

    public void setAction(DatabaseAction action) {
        this.action = action;
    }

    @Override
    public String toString() {
        return "DatabaseQuery{" +
                "action=" + action +
                ", originator='" + originator + '\'' +
                ", database='" + database + '\'' +
                ", collection='" + collection + '\'' +
                '}';
    }
}
