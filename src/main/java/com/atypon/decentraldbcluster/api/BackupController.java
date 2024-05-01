package com.atypon.decentraldbcluster.api;

import com.atypon.decentraldbcluster.storage.disk.FileSystemService;
import com.atypon.decentraldbcluster.utility.PathConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

//Protected by AdminFilter
@RestController
@RequestMapping("/api/backup")
@CrossOrigin("*")
public class BackupController {

    private final FileSystemService fileSystemService;

    @Autowired
    public BackupController(FileSystemService fileSystemService) {
        this.fileSystemService = fileSystemService;
    }

    @GetMapping("/downloadBackup")
    public ResponseEntity<InputStreamResource> downloadBackup() throws IOException {
        String sourceDirPath = PathConstructor.getRootDirectory();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ZipOutputStream zos = new ZipOutputStream(baos);

        Path sourceFolderPath = Paths.get(sourceDirPath);
        Files.walk(sourceFolderPath)
                .filter(path -> !Files.isDirectory(path))
                .forEach(path -> {
                    ZipEntry zipEntry = new ZipEntry(sourceFolderPath.relativize(path).toString());
                    try {
                        zos.putNextEntry(zipEntry);
                        Files.copy(path, zos);
                        zos.closeEntry();
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to package file.", e);
                    }
                });

        zos.finish();
        zos.close();
        byte[] bytes = baos.toByteArray();

        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"node-storage.zip\"")
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .body(new InputStreamResource(new ByteArrayInputStream(bytes)));
    }


    @PostMapping("/uploadBackup")
    public ResponseEntity<?> uploadBackup(@RequestBody MultipartFile file) throws IOException {

        String rootDirectory = PathConstructor.getRootDirectory();
        try {
            fileSystemService.deleteDirectory(rootDirectory);
        } catch (Exception ignored){}

        if (file.isEmpty()) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("No file uploaded");
        }

        // Ensure the storage directory exists
        Path destinationDir = Paths.get(rootDirectory);
        if (!Files.exists(destinationDir)) {
            Files.createDirectories(destinationDir);
        }

        // Extract the ZIP file directly from the input stream
        try (ZipInputStream zis = new ZipInputStream(file.getInputStream())) {
            ZipEntry zipEntry = zis.getNextEntry();
            while (zipEntry != null) {
                Path newFile = destinationDir.resolve(zipEntry.getName());
                if (zipEntry.isDirectory()) {
                    if (!Files.exists(newFile)) {
                        Files.createDirectories(newFile);
                    }
                } else {
                    // Ensure parent directory for the entry is created
                    if (newFile.getParent() != null && !Files.exists(newFile.getParent())) {
                        Files.createDirectories(newFile.getParent());
                    }
                    Files.copy(zis, newFile);
                }
                zipEntry = zis.getNextEntry();
            }
            zis.closeEntry();
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed to extract ZIP file");
        }

        return ResponseEntity.ok().build();
    }

}
