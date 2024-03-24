package com.atypon.decentraldbcluster.api.controller;

import com.atypon.decentraldbcluster.services.DocumentService;
import com.atypon.decentraldbcluster.services.FileStorageService;
import com.atypon.decentraldbcluster.services.IndexService;
import com.atypon.decentraldbcluster.services.UserDetails;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/index")
@CrossOrigin("*")
public class IndexController {

    private final UserDetails userDetails;
    private final IndexService indexService;
    private final DocumentService documentService;


    @Autowired
    public IndexController(UserDetails userDetails, IndexService indexService, DocumentService documentService) {
        this.userDetails = userDetails;
        this.indexService = indexService;
        this.documentService = documentService;
    }

    @PostMapping("{database}/{collection}/createIndex/{field}")
    public void createIndex(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @PathVariable String field) throws Exception {

        String userDirectory = userDetails.getUserDirectory(request);
        String collectionPath = FileStorageService.constructCollectionPath(userDirectory, database, collection);

        var documents = documentService.readAllDocumentsInCollection(collectionPath, null);

        indexService.createIndex(documents, collectionPath, field);
    }

    @DeleteMapping("{database}/{collection}/deleteIndex/{field}")
    public void deleteIndex(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @PathVariable String field) throws Exception {

        String userDirectory = userDetails.getUserDirectory(request);
        String collectionPath = FileStorageService.constructCollectionPath(userDirectory, database, collection);
        String indexPath = indexService.constructIndexPath(collectionPath, field);
        FileStorageService.deleteFile(indexPath);
    }

}
