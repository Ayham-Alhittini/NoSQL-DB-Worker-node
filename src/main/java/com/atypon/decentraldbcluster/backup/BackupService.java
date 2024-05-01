package com.atypon.decentraldbcluster.backup;

import com.atypon.decentraldbcluster.storage.disk.FileSystemService;
import com.atypon.decentraldbcluster.storage.managers.DocumentStorageManager;
import com.atypon.decentraldbcluster.storage.managers.IndexStorageManager;
import com.atypon.decentraldbcluster.utility.PathConstructor;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

@Service
public class BackupService {

    private final FileSystemService fileSystemService;
    private final DocumentStorageManager documentStorageManager;
    private final IndexStorageManager indexStorageManager;

    public BackupService(FileSystemService fileSystemService, DocumentStorageManager documentStorageManager, IndexStorageManager indexStorageManager) {
        this.fileSystemService = fileSystemService;
        this.documentStorageManager = documentStorageManager;
        this.indexStorageManager = indexStorageManager;
    }

    public byte[] createBackup() throws IOException {
        Path sourceFolderPath = Paths.get(PathConstructor.getRootDirectory());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ZipOutputStream zos = new ZipOutputStream(baos)) {
            Files.walk(sourceFolderPath).forEach(path -> addPathToZip(zos, sourceFolderPath, path));
        }
        return baos.toByteArray();
    }

    private void addPathToZip(ZipOutputStream zos, Path sourceFolderPath, Path filePath) {
        try {
            String zipEntryName = sourceFolderPath.relativize(filePath).toString();
            if (Files.isDirectory(filePath)) {
                zipEntryName += "/";
            }
            zos.putNextEntry(new ZipEntry(zipEntryName));
            if (!Files.isDirectory(filePath)) {
                Files.copy(filePath, zos);
            }
            zos.closeEntry();
        } catch (IOException e) {
            throw new RuntimeException("Failed to package file.", e);
        }
    }

    public void restoreBackup(MultipartFile backupFile) throws IOException {
        Path rootDir = Paths.get(PathConstructor.getRootDirectory());
        clearStorage();
        ensureDirectoryExists(rootDir);

        try (ZipInputStream zis = new ZipInputStream(backupFile.getInputStream())) {
            ZipEntry zipEntry;
            while ((zipEntry = zis.getNextEntry()) != null) {
                restoreZipEntry(rootDir, zipEntry, zis);
            }
        }
    }

    private void restoreZipEntry(Path destinationDir, ZipEntry zipEntry, ZipInputStream zis) throws IOException {
        Path newFilePath = destinationDir.resolve(zipEntry.getName());
        if (zipEntry.isDirectory()) {
            ensureDirectoryExists(newFilePath);
        } else {
            Files.copy(zis, newFilePath);
        }
    }

    private void ensureDirectoryExists(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            Files.createDirectories(directory);
        }
    }

    private void clearStorage() throws IOException {
        fileSystemService.deleteDirectory(PathConstructor.getRootDirectory());
        documentStorageManager.clearDocumentStorageCache();
        indexStorageManager.clearIndexStorageCache();
    }
}
