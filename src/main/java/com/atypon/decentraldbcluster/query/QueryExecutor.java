package com.atypon.decentraldbcluster.query;

import com.atypon.decentraldbcluster.query.base.Executable;
import com.atypon.decentraldbcluster.query.base.Query;
import com.atypon.decentraldbcluster.query.collections.CollectionQuery;
import com.atypon.decentraldbcluster.query.collections.CollectionQueryExecutor;
import com.atypon.decentraldbcluster.query.databases.DatabaseQuery;
import com.atypon.decentraldbcluster.query.databases.DatabaseQueryExecutor;
import com.atypon.decentraldbcluster.query.documents.DocumentQuery;
import com.atypon.decentraldbcluster.query.documents.DocumentQueryExecutor;
import com.atypon.decentraldbcluster.query.index.IndexQuery;
import com.atypon.decentraldbcluster.query.index.IndexQueryExecutor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class QueryExecutor implements Executable<Query> {

    private final IndexQueryExecutor indexQueryExecutor;
    private final DocumentQueryExecutor documentQueryExecutor;
    private final DatabaseQueryExecutor databaseQueryExecutor;
    private final CollectionQueryExecutor collectionQueryExecutor;

    @Autowired
    public QueryExecutor(DatabaseQueryExecutor databaseQueryExecutor, CollectionQueryExecutor collectionQueryExecutor,
                         IndexQueryExecutor indexQueryExecutor, DocumentQueryExecutor documentQueryExecutor) {
        this.indexQueryExecutor = indexQueryExecutor;
        this.documentQueryExecutor = documentQueryExecutor;
        this.databaseQueryExecutor = databaseQueryExecutor;
        this.collectionQueryExecutor = collectionQueryExecutor;
    }


    public <R> R exec(Query query, Class<R> returnType) throws Exception {
        Object result = exec(query);

        if (returnType.isInstance(result))
            return returnType.cast(result);
        throw new ClassCastException("The result cannot be cast to " + returnType.getName());
    }



    // Factory design pattern
    @Override
    public Object exec(Query query) throws Exception {

        if (query instanceof DatabaseQuery)
            return databaseQueryExecutor.exec((DatabaseQuery) query);

        else if (query instanceof CollectionQuery)
            return collectionQueryExecutor.exec((CollectionQuery) query);

        else if (query instanceof DocumentQuery)
            return documentQueryExecutor.exec((DocumentQuery) query);

        else if (query instanceof IndexQuery)
            return indexQueryExecutor.exec((IndexQuery) query);

        throw new UnsupportedOperationException("Unexpected query type, please attach it to the factory at the QueryExecutor if new Query added");
    }

}
