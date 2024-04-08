package com.atypon.decentraldbcluster.services;

import com.atypon.decentraldbcluster.disk.FileSystemService;
import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.query.types.DocumentQuery;
import com.atypon.decentraldbcluster.utility.PathConstructor;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Service
public class DocumentReaderService {

    private final ObjectMapper mapper;
    private final FileSystemService fileSystemService;

    @Autowired
    public DocumentReaderService(ObjectMapper mapper, FileSystemService fileSystemService) {
        this.mapper = mapper;
        this.fileSystemService = fileSystemService;
    }

    public Document readDocument(String documentPath) throws IOException {
        String fileContent = fileSystemService.loadFileContent(documentPath);
        return mapper.readValue(fileContent, Document.class);
    }

    public Document findDocumentById(DocumentQuery query) throws Exception {
        String documentPath = PathConstructor.constructDocumentPath(query);
        return readDocument(documentPath);
    }

    public List<Document> readDocumentsByDocumentsPathList(Set<String> documentsPath) throws IOException {
        List<Document> documents = new ArrayList<>();
        for (var documentPath: documentsPath) {
            documents.add( readDocument(documentPath) );
        }
        return documents;
    }


    public List<Document> readDocumentsByCollectionPath(String collectionPath) throws IOException {
        List<Document> documents = new ArrayList<>();
        List<String> filesPath = fileSystemService.getDirectoryFilesPath( Paths.get(collectionPath, "documents").toString() );
        for (String documentPath: filesPath) {
            documents.add( readDocument(documentPath) );
        }
        return documents;
    }
}
