package com.atypon.decentraldbcluster.api.external;

import com.atypon.decentraldbcluster.services.FileSystemService;
import com.atypon.decentraldbcluster.services.PathConstructor;
import com.atypon.decentraldbcluster.services.UserDetails;
import com.atypon.decentraldbcluster.services.DocumentIndexService;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/index")
@CrossOrigin("*")
public class IndexController {

    private final UserDetails userDetails;
    private final DocumentIndexService documentIndexService;
    private final FileSystemService fileSystemService;


    @Autowired
    public IndexController(UserDetails userDetails, DocumentIndexService documentIndexService, FileSystemService fileSystemService) {
        this.userDetails = userDetails;
        this.documentIndexService = documentIndexService;
        this.fileSystemService = fileSystemService;
    }

    @PostMapping("{database}/{collection}/createIndex/{field}")
    public void createIndex(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @PathVariable String field) throws Exception {

        String userDirectory = userDetails.getUserDirectory(request);
        String collectionPath = PathConstructor.constructCollectionPath(userDirectory, database, collection);

        // TODO: handle field not exists
        documentIndexService.createIndex(collectionPath, field);
    }

    @DeleteMapping("{database}/{collection}/deleteIndex/{field}")
    public void deleteIndex(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @PathVariable String field) throws Exception {

        String userDirectory = userDetails.getUserDirectory(request);
        String collectionPath = PathConstructor.constructCollectionPath(userDirectory, database, collection);
        String indexPath = PathConstructor.constructUserGeneratedIndexPath(collectionPath, field);

        fileSystemService.deleteFile(indexPath);
    }

}
