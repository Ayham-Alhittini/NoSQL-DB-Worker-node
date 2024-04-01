package com.atypon.decentraldbcluster.api.external;

import com.atypon.decentraldbcluster.services.BroadcastService;
import com.atypon.decentraldbcluster.services.FileSystemService;
import com.atypon.decentraldbcluster.services.PathConstructor;
import com.atypon.decentraldbcluster.services.UserDetails;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpMethod;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

@RestController
@RequestMapping("/api/database")
@CrossOrigin("*")
public class DatabaseController {


    private final UserDetails userDetails;
    private final FileSystemService fileSystemService;

    @Autowired
    public DatabaseController(UserDetails userDetails, FileSystemService fileSystemService) {
        this.userDetails = userDetails;
        this.fileSystemService = fileSystemService;
    }

    @PostMapping("/create/{database}")
    public void createDatabase(HttpServletRequest request, @PathVariable String database) throws IOException {

        String rootDirectory = PathConstructor.getRootDirectory();
        String userDirectory = userDetails.getUserDirectory(request);

        String databasePath = Paths.get(rootDirectory, userDirectory, database).toString();
        fileSystemService.createDirectory(databasePath);

        BroadcastService.doBroadcast(request, "createDB/" + database, null, HttpMethod.POST);
    }

    @DeleteMapping("/delete/{database}")
    public void deleteDatabase(HttpServletRequest request, @PathVariable String database) throws IOException {

        String rootDirectory = PathConstructor.getRootDirectory();
        String userDirectory = userDetails.getUserDirectory(request);

        String databasePath = Paths.get(rootDirectory, userDirectory, database).toString();
        fileSystemService.deleteDirectory( databasePath);

        BroadcastService.doBroadcast(request, "dropDB/" + database, null, HttpMethod.DELETE);
    }

    @GetMapping("/showDbs")
    public List<String> showDbs(HttpServletRequest request) {

        String rootDirectory = PathConstructor.getRootDirectory();
        String userDirectory = userDetails.getUserDirectory(request);

        String userPath = Paths.get(rootDirectory, userDirectory).toString();

        return fileSystemService.listAllDirectories(userPath);
    }
}
