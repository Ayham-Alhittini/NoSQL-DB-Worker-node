package com.atypon.decentraldbcluster.api;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

@RestController
@RequestMapping("/api/test")
public class TestController {
    private final ConcurrentHashMap<String, AtomicInteger> documentVersions;

    public TestController() {
        documentVersions = new ConcurrentHashMap<>();
        documentVersions.put("doc1", new AtomicInteger(1));
        documentVersions.put("doc2", new AtomicInteger(1));
        documentVersions.put("doc3", new AtomicInteger(1));
    }

    @GetMapping
    public List<String> get(@RequestParam String documentId, @RequestParam int expectedVersion) throws InterruptedException {
        List<String> response = new ArrayList<>();
        int numberOfThreads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);

        // A latch to ensure all threads start at the same time for maximum concurrency
        CountDownLatch startLatch = new CountDownLatch(1);
        // A latch to wait for all threads to finish
        CountDownLatch endLatch = new CountDownLatch(numberOfThreads);

        // Simulate concurrent updates to "doc1"
        IntStream.range(0, numberOfThreads).forEach(i -> executor.submit(() -> {
            try {
                // Wait for the signal to start
                startLatch.await();

                boolean updated = updateDocumentVersion(documentId, expectedVersion);
                if (updated) {
                    response.add("Thread " + i + " successfully updated doc1");
                    response.add(documentVersions.get("doc1").get() + "");
                } else {
                    response.add("Thread " + i + " failed to update doc1 due to version mismatch");
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                endLatch.countDown();
            }
        }));

        // Start all threads
        startLatch.countDown();
        // Wait for all threads to finish
        endLatch.await();
        executor.shutdown();
        return response;
    }


    private boolean updateDocumentVersion(String documentId, int expectedVersion) {
        AtomicInteger currentVersion = documentVersions.get(documentId);
        return currentVersion.compareAndSet(expectedVersion, expectedVersion + 1);
    }
}
