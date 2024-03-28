package com.atypon.decentraldbcluster.api;

import com.atypon.decentraldbcluster.lock.DiskResourcesLock;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CyclicBarrier;

@RestController
@RequestMapping("/api/test")
public class TestController {

    private final CyclicBarrier waiter = new CyclicBarrier(2);
    private final DiskResourcesLock resourcesLock = new DiskResourcesLock();
    public TestController() {

    }

    @GetMapping("/hold")
    public void holdWrite() {
        resourcesLock.lockWriteResource("resource1");
    }

    @GetMapping("/get")
    public String getRead() {
        resourcesLock.lockReadResource("resource1");
        try {
            return "This is read content";
        } finally {
            resourcesLock.releaseReadResource("resource1");
        }
    }

}
