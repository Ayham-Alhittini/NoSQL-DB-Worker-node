package com.atypon.decentraldbcluster.query.builder;

import com.atypon.decentraldbcluster.query.actions.DatabaseAction;
import com.atypon.decentraldbcluster.query.types.DatabaseQuery;

public class DatabaseQueryBuilder implements QueryBuilder {
    private final DatabaseQuery query = new DatabaseQuery();

    public DatabaseQueryBuilder createDatabase(String database) {
        query.setDatabaseAction(DatabaseAction.CREATE);
        query.setDatabase(database);
        return this;
    }

    public DatabaseQueryBuilder dropDatabase(String database) {
        query.setDatabaseAction(DatabaseAction.DROP);
        query.setDatabase(database);
        return this;
    }

    public DatabaseQueryBuilder showDbs() {
        query.setDatabaseAction(DatabaseAction.SHOW);
        return this;
    }

    @Override
    public DatabaseQueryBuilder withOriginator(String originator) {
        query.setOriginator(originator);
        return this;
    }

    @Override
    public DatabaseQueryBuilder withDatabase(String database) {
        throw new UnsupportedOperationException("Use add, remove database for database query");
    }

    @Override
    public DatabaseQueryBuilder withCollection(String collection) {
        throw new UnsupportedOperationException("Collection have nothing to do in database query");
    }

    @Override
    public DatabaseQuery build() {
        return query;
    }
}
