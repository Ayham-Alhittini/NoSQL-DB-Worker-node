package com.atypon.decentraldbcluster;

import com.atypon.decentraldbcluster.storage.disk.FileSystemService;
import com.atypon.decentraldbcluster.utility.PathConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;

@SpringBootApplication
public class WorkerNodeApplication {

    @Autowired
	public WorkerNodeApplication(FileSystemService fileSystemService) throws IOException {
		fileSystemService.createDirectory(PathConstructor.getRootDirectory());
    }

	public static void main(String[] args) {
		SpringApplication.run(WorkerNodeApplication.class, args);
	}

}
