package com.atypon.decentraldbcluster.api.controller;

import com.atypon.decentraldbcluster.services.FileStorageService;
import com.atypon.decentraldbcluster.services.UserDetails;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
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

    @PostMapping("/create/{databaseName}")
    public ResponseEntity<?> createDatabase(HttpServletRequest request, @PathVariable String databaseName) throws IOException {

        FileStorageService.createDirectory( userDetails.getUserId(request) + "/" + databaseName);

        return ResponseEntity.ok().build();
    }

    @DeleteMapping("/delete/{databaseName}")
    public ResponseEntity<?> deleteDatabase(HttpServletRequest request, @PathVariable String databaseName) throws IOException {

        FileStorageService.deleteDirectory( userDetails.getUserId(request) + "/" + databaseName);

        return ResponseEntity.ok().build();
    }

    @GetMapping("/showDbs")
    public ResponseEntity<?> showDbs(HttpServletRequest request) {

        List<String> dbs = FileStorageService.listAllDirectories(userDetails.getUserId(request));

        return ResponseEntity.ok(dbs);

    }
}
