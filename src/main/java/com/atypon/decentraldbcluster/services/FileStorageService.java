package com.atypon.decentraldbcluster.services;


import com.atypon.decentraldbcluster.error.ResourceNotFoundException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

public class FileStorageService {

    private static final String baseDirectory = "./storage/";


    //config
    public static String getRootDirectory() {
        return baseDirectory;
    }

    public static String constructCollectionPath(String userDirectory, String database, String collection) {
        return Paths.get(getRootDirectory(), userDirectory, database, collection).toString();
    }

    public static void createDirectory(String directoryPath) throws IOException {
        Path nodeStorageDir = Paths.get(directoryPath);

        if (!Files.exists(nodeStorageDir)) {
            Files.createDirectories(nodeStorageDir);
        }
    }

    public static void deleteDirectory(String directoryPath) throws IOException {
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

    public static void deleteFile(String filePath) throws IOException {
        Path path = Paths.get(filePath);

        if (!Files.exists(path))
            throw new ResourceNotFoundException("File not exists");

        Files.delete(path);
    }

    public static void createBaseStorageDirectory() {
        try {
            createDirectory( getRootDirectory() );
        } catch (IOException e) {
            System.out.println("Can't create base directory" + e.getMessage());
        }
    }

    public static List<String> listAllDirectories(String path) {

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

    public static void saveFile(String data, String path) throws IOException {
        try (FileWriter file = new FileWriter(path)) {
            file.write(data);
            file.flush();
        }
    }
}
