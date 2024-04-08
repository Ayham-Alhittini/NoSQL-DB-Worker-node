package com.atypon.decentraldbcluster.query.handlers.document;

import com.atypon.decentraldbcluster.disk.FileSystemService;
import com.atypon.decentraldbcluster.query.types.DocumentQuery;
import com.atypon.decentraldbcluster.services.DocumentIndexService;
import com.atypon.decentraldbcluster.utility.PathConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DeleteDocumentHandler {
    private final FileSystemService fileSystemService;
    private final DocumentIndexService documentIndexService;
    @Autowired
    public DeleteDocumentHandler(FileSystemService fileSystemService, DocumentIndexService documentIndexService) {
        this.fileSystemService = fileSystemService;
        this.documentIndexService = documentIndexService;
    }

    public void handle(DocumentQuery query) throws Exception {

        String collectionPath = PathConstructor.constructCollectionPath(query);
        String documentPath = PathConstructor.constructDocumentPath(collectionPath, query.getDocumentId());

        documentIndexService.deleteDocumentFromIndexes(documentPath);
        fileSystemService.deleteFile(documentPath);
    }

}
