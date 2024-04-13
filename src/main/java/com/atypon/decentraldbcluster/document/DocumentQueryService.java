package com.atypon.decentraldbcluster.document;

import com.atypon.decentraldbcluster.disk.FileSystemService;
import com.atypon.decentraldbcluster.persistence.DocumentPersistenceManager;
import com.atypon.decentraldbcluster.query.types.DocumentQuery;
import com.atypon.decentraldbcluster.utility.PathConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Service
public class DocumentQueryService {

    private final FileSystemService fileSystemService;
    private final DocumentPersistenceManager documentPersistenceManager;

    @Autowired
    public DocumentQueryService(FileSystemService fileSystemService, DocumentPersistenceManager documentPersistenceManager) {
        this.fileSystemService = fileSystemService;
        this.documentPersistenceManager = documentPersistenceManager;
    }


    public Document findDocumentById(DocumentQuery query) throws Exception {
        String documentPath = PathConstructor.constructDocumentPath(query);
        return documentPersistenceManager.loadDocument(documentPath);
    }


    public List<Document> readDocumentsByDocumentsPathList(Set<String> documentsPath) throws Exception {
        List<Document> documents = new ArrayList<>();
        for (var documentPath: documentsPath) {
            documents.add( documentPersistenceManager.loadDocument(documentPath) );
        }
        return documents;
    }


    public List<Document> readDocumentsByCollectionPath(String collectionPath) throws Exception {
        List<Document> documents = new ArrayList<>();
        List<String> filesPath = fileSystemService.getDirectoryFilesPath( Paths.get(collectionPath, "documents").toString() );
        for (String documentPath: filesPath) {
            documents.add( documentPersistenceManager.loadDocument(documentPath) );
        }
        return documents;
    }
}
