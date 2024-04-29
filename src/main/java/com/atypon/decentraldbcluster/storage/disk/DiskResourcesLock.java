package com.atypon.decentraldbcluster.storage.disk;

import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Service
public class DiskResourcesLock {
    private static final ConcurrentHashMap<String, ReadWriteLock> resourcesLock = new ConcurrentHashMap<>();

    public void lockReadResource(String resourcePath) {
        getLock(resourcePath).readLock().lock();
    }

    public void releaseReadResource(String resourcePath) {
        ReadWriteLock lock = resourcesLock.get(resourcePath);
        lock.readLock().unlock();
    }

    public void lockWriteResource(String resourcePath) {
        getLock(resourcePath).writeLock().lock();
    }

    public void releaseWriteResource(String resourcePath) {
        ReadWriteLock lock = resourcesLock.get(resourcePath);
        lock.writeLock().unlock();
    }

    private ReadWriteLock getLock(String resourcePath) {
        return resourcesLock.computeIfAbsent(resourcePath, ignored -> new ReentrantReadWriteLock());
    }
}
