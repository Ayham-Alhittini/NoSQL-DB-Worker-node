package com.atypon.decentraldbcluster.query.handlers.document;

import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.storage.managers.DocumentStorageManager;
import com.atypon.decentraldbcluster.query.types.DocumentQuery;
import com.atypon.decentraldbcluster.storage.managers.IndexStorageManager;
import com.atypon.decentraldbcluster.utility.IndexUtil;
import com.atypon.decentraldbcluster.utility.PathConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DeleteDocumentHandler {
    private final IndexStorageManager indexStorageManager;
    private final DocumentStorageManager documentStorageManager;
    @Autowired
    public DeleteDocumentHandler(DocumentStorageManager documentPersistenceManager, IndexStorageManager indexStorageManager) {
        this.documentStorageManager = documentPersistenceManager;
        this.indexStorageManager = indexStorageManager;
    }

    public void handle(DocumentQuery query) throws Exception {

        String collectionPath = PathConstructor.constructCollectionPath(query);
        String documentPath = PathConstructor.constructDocumentPath(collectionPath, query.getDocumentId());

        deleteDocumentFromIndexes(documentPath);
        documentStorageManager.removeDocument(documentPath);
    }

    public void deleteDocumentFromIndexes(String documentPointer) throws Exception {
        Document document = documentStorageManager.loadDocument(documentPointer);
        String collectionPath = PathConstructor.extractCollectionPathFromDocumentPath(documentPointer);
        List<String> indexedFields = IndexUtil.getIndexedFields(document.getContent(), collectionPath);

        for (String field : indexedFields) {
            String indexPath = PathConstructor.constructIndexPath(collectionPath, field);
            indexStorageManager.removeFromIndex(indexPath, document.getContent().get(field), documentPointer);
        }
    }

}
