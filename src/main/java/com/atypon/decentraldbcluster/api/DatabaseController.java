package com.atypon.decentraldbcluster.api;

import com.atypon.decentraldbcluster.services.FileStorageService;
import com.atypon.decentraldbcluster.services.PathConstructor;
import com.atypon.decentraldbcluster.services.UserDetails;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

@RestController
@RequestMapping("/api/database")
@CrossOrigin("*")
public class DatabaseController {


    private final UserDetails userDetails;

    @Autowired
    public DatabaseController(UserDetails userDetails) {
        this.userDetails = userDetails;
    }

    @PostMapping("/create/{database}")
    public void createDatabase(HttpServletRequest request, @PathVariable String database) throws IOException {

        String rootDirectory = PathConstructor.getRootDirectory();
        String userDirectory = userDetails.getUserDirectory(request);

        String databasePath = Paths.get(rootDirectory, userDirectory, database).toString();
        FileStorageService.createDirectory( databasePath);

    }

    @DeleteMapping("/delete/{database}")
    public void deleteDatabase(HttpServletRequest request, @PathVariable String database) throws IOException {

        String rootDirectory = PathConstructor.getRootDirectory();
        String userDirectory = userDetails.getUserDirectory(request);

        String databasePath = Paths.get(rootDirectory, userDirectory, database).toString();
        FileStorageService.deleteDirectory( databasePath);

    }

    @GetMapping("/showDbs")
    public ResponseEntity<List<String>> showDbs(HttpServletRequest request) {

        String rootDirectory = PathConstructor.getRootDirectory();
        String userDirectory = userDetails.getUserDirectory(request);

        String userPath = Paths.get(rootDirectory, userDirectory).toString();

        List<String> dbs = FileStorageService.listAllDirectories(userPath);

        return ResponseEntity.ok(dbs);

    }
}
