package com.atypon.decentraldbcluster.api;

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


    @Autowired
    public IndexController(UserDetails userDetails, IndexService indexService) {
        this.userDetails = userDetails;
        this.indexService = indexService;
    }

    @PostMapping("{database}/{collection}/createIndex/{field}")
    public void createIndex(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @PathVariable String field) throws Exception {

        String userDirectory = userDetails.getUserDirectory(request);
        String collectionPath = FileStorageService.constructCollectionPath(userDirectory, database, collection);

        // TODO: handle field not exists
        indexService.createIndex(collectionPath, field);
    }

    @DeleteMapping("{database}/{collection}/deleteIndex/{field}")
    public void deleteIndex(HttpServletRequest request, @PathVariable String database, @PathVariable String collection, @PathVariable String field) throws Exception {

        String userDirectory = userDetails.getUserDirectory(request);
        String collectionPath = FileStorageService.constructCollectionPath(userDirectory, database, collection);
        String indexPath = indexService.constructUserGeneratedIndexesPath(collectionPath, field);
        FileStorageService.deleteFile(indexPath);
    }

}
