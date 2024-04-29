package com.atypon.decentraldbcluster.utility;

import com.atypon.decentraldbcluster.query.types.DocumentQuery;
import com.atypon.decentraldbcluster.query.types.Query;

import java.nio.file.Paths;

public class PathConstructor {
    private static final String baseDirectory = "./node-storage/";

    public static String getRootDirectory() {
        return baseDirectory;
    }

    public static String constructCollectionPath(String userDirectory, String database, String collection) {
        return Paths.get(getRootDirectory(), userDirectory, database, collection).toString();
    }
    public static String constructCollectionPath(Query query) {
        return Paths.get(getRootDirectory(), query.getOriginator(), query.getDatabase(), query.getCollection()).toString();
    }

    public static String constructIndexPath(String collectionPath, String fieldName) {
        return Paths.get(collectionPath, "indexes", fieldName + ".ser").toString();
    }

    public static String constructDocumentPath(String collectionPath, String documentId) {
        return Paths.get( collectionPath , "documents", documentId + ".json").toString();
    }

    public static String constructDocumentPath(DocumentQuery query) {
        return constructDocumentPath( constructCollectionPath(query), query.getDocumentId());
    }

    public static String extractCollectionPathFromDocumentPath(String documentPath) {
        return documentPath.substring(0, documentPath.indexOf("documents"));
    }
}
