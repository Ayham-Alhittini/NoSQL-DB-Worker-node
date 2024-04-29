package com.atypon.decentraldbcluster.storage.managers;

import com.atypon.decentraldbcluster.cache.document.DocumentCache;
import com.atypon.decentraldbcluster.storage.disk.FileSystemService;
import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.utility.JsonUtil;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@Service
public class DocumentStorageManager {
    private final JsonUtil jsonUtil;
    private final DocumentCache documentCache;
    private final FileSystemService fileSystemService;

    public DocumentStorageManager(JsonUtil jsonUtil, FileSystemService fileSystemService, DocumentCache documentCache) {
        this.jsonUtil = jsonUtil;
        this.fileSystemService = fileSystemService;
        this.documentCache = documentCache;
    }

    public void saveDocument(String documentPath, Document document) throws IOException {
        String json = jsonUtil.toJsonString(document);
        fileSystemService.saveFile(json, documentPath);
        documentCache.putDocument(documentPath, document);
    }

    public Document loadDocument(String documentPath) throws Exception {
        if (documentCache.isDocumentCached(documentPath)) {
            return documentCache.getDocument(documentPath);
        }
        String documentContent = fileSystemService.loadFileContent(documentPath);
        Document document = jsonUtil.fromJsonString(documentContent);
        documentCache.putDocument(documentPath, document);
        return document;
    }

    public void removeDocument(String documentPath) throws IOException {
        fileSystemService.deleteFile(documentPath);
        documentCache.removeDocument(documentPath);
    }

    public List<Document> getCollectionDocuments(String collectionPath) throws Exception {
        List<Document> documents = new ArrayList<>();
        List<String> filesPath = fileSystemService.getDirectoryFilesPath( Paths.get(collectionPath, "documents").toString() );
        for (String documentPath: filesPath) {
            documents.add( loadDocument(documentPath) );
        }
        return documents;
    }

}
