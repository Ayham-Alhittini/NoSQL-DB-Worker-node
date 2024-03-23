package com.atypon.decentraldbcluster;

import com.atypon.decentraldbcluster.services.FileStorageService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class WorkerNodeApplication {

	public static void main(String[] args) {
		FileStorageService.createBaseStorageDirectory();
		SpringApplication.run(WorkerNodeApplication.class, args);
	}

}
