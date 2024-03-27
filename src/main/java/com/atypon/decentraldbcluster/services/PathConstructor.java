package com.atypon.decentraldbcluster.services;

import java.nio.file.Paths;

public class PathConstructor {
    private static final String baseDirectory = "./storage/";

    public static String getRootDirectory() {
        return baseDirectory;
    }

    public static String constructCollectionPath(String userDirectory, String database, String collection) {
        return Paths.get(getRootDirectory(), userDirectory, database, collection).toString();
    }

    public static String constructUserGeneratedIndexPath(String collectionPath, String fieldName) {
        return Paths.get(collectionPath, "indexes", "user_generated_indexes", fieldName + ".ser").toString();
    }

    public static String constructSystemGeneratedIndexPath(String collectionPath) {
        return Paths.get(collectionPath, "indexes", "system_generated_indexes", "id.ser").toString();
    }

    public static String constructDocumentPath(String collectionPath, String documentId) {
        return Paths.get( collectionPath , "documents", documentId + ".json").toString();
    }

    public static String getCollectionPathFromDocumentPath(String documentPath) {
        return documentPath.substring(0, documentPath.indexOf("documents"));
    }

}
