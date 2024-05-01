package com.atypon.decentraldbcluster.storage.disk;


import com.atypon.decentraldbcluster.exceptions.types.ResourceNotFoundException;
import com.atypon.decentraldbcluster.index.Index;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

@Service
public class FileSystemService {
    private final DiskResourcesLock resourcesLock;
    @Autowired
    public FileSystemService(DiskResourcesLock resourcesLock) {
        this.resourcesLock = resourcesLock;
    }

    //------------------------- Non-concurrent methods, done by developer level, not application level

    public void createDirectory(String directoryPath) throws IOException {
        Path nodeStorageDir = Paths.get(directoryPath);

        if (!Files.exists(nodeStorageDir)) {
            Files.createDirectories(nodeStorageDir);
        }
    }

    public void deleteDirectory(String directoryPath) throws IOException {
        Path dir = Paths.get(directoryPath);

        if (!Files.exists(dir))
            throw new IllegalArgumentException("Directory not exists");

        try (Stream<Path> stream = Files.walk(dir)) {
            stream.sorted(Comparator.reverseOrder())
                .forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
        }
    }

    public List<String> getAllDirectories(String path) {

        List<String> directoriesName = new ArrayList<>();

        File directory = new File(path);

        if (directory.isFile())
            throw new IllegalArgumentException("Expect directory");

        File [] files = directory.listFiles();

        if (files == null)
            return new ArrayList<>();

        for (File innerDirectory: files) if (innerDirectory.isDirectory())
            directoriesName.add(innerDirectory.getName());

        return directoriesName;
    }

    //------------------------- Concurrent but safe, no lock needed

    public List<String> getDirectoryFilesPath(String directoryPath) {
        List<String> filenames = new ArrayList<>();
        try (Stream<Path> paths = Files.list(Paths.get(directoryPath))) {
            paths.forEach(path -> filenames.add(path.toString()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return filenames;
    }


    //------------------------- Concurrent methods need to locked

    public void deleteFile(String filePath) throws IOException {
        Path path = Paths.get(filePath);

        if (!Files.exists(path))
            throw new ResourceNotFoundException("File not exists");

        resourcesLock.lockWriteResource(filePath);
        try {
            Files.delete(path);
        } finally {
            resourcesLock.releaseWriteResource(filePath);
        }

    }

    public boolean isFileExists(String filePath) {
        return Files.isRegularFile(Path.of(filePath));
    }

    public String loadFileContent(String filePath) {
        resourcesLock.lockReadResource(filePath);
        try {
            Path path = Paths.get(filePath);

            if (Files.isRegularFile(path)) {
                return Files.readString(path);
            }
            throw new ResourceNotFoundException("File not exists");
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            resourcesLock.releaseReadResource(filePath);
        }
    }

    public void saveFile(String data, String filePath) throws IOException {
        resourcesLock.lockWriteResource(filePath);
        try (FileWriter file = new FileWriter(filePath)) {
            file.write(data);
            file.flush();
        } finally {
            resourcesLock.releaseWriteResource(filePath);
        }
    }

    public Index loadIndex(String indexPath) throws Exception {
        resourcesLock.lockReadResource(indexPath);
        try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(indexPath))) {
            return (Index) in.readObject();
        } finally {
            resourcesLock.releaseReadResource(indexPath);
        }
    }

    public void saveIndex(Index index, String indexPath) throws IOException {
        resourcesLock.lockWriteResource(indexPath);
        try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(indexPath))) {
            out.writeObject(index);
        }  finally {
            resourcesLock.releaseWriteResource(indexPath);
        }
    }

}
