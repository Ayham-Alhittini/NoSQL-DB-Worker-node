package com.atypon.decentraldbcluster.persistence;

import com.atypon.decentraldbcluster.cache.document.DocumentCache;
import com.atypon.decentraldbcluster.disk.FileSystemService;
import com.atypon.decentraldbcluster.document.entity.Document;
import com.atypon.decentraldbcluster.utility.JsonUtil;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class DocumentPersistenceManager {
    private final JsonUtil jsonUtil;
    private final DocumentCache documentCache;
    private final FileSystemService fileSystemService;

    public DocumentPersistenceManager(JsonUtil jsonUtil, FileSystemService fileSystemService, DocumentCache documentCache) {
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

}
