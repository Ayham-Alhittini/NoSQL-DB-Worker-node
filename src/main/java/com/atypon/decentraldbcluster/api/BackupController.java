package com.atypon.decentraldbcluster.api;

import com.atypon.decentraldbcluster.backup.BackupService;
import com.atypon.decentraldbcluster.communication.braodcast.BroadcastService;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayInputStream;
import java.io.IOException;

@RestController
@RequestMapping("/api/backup")
@CrossOrigin("*")
public class BackupController {

    private final BackupService backupService;
    private final BroadcastService broadcastService;


    public BackupController(BroadcastService broadcastService, BackupService backupService) {
        this.broadcastService = broadcastService;
        this.backupService = backupService;
    }

    @GetMapping("/downloadBackup")
    public ResponseEntity<InputStreamResource> downloadBackup() throws IOException {

        byte[] bytes = backupService.createBackup();

        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"node-storage.zip\"")
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .body(new InputStreamResource(new ByteArrayInputStream(bytes)));
    }


    @PostMapping("/uploadBackup")
    public void uploadBackup(@RequestBody MultipartFile backupFile) throws IOException {
        backupService.restoreBackup(backupFile);
        broadcastService.doBackupBroadcast(backupFile);
    }

}
