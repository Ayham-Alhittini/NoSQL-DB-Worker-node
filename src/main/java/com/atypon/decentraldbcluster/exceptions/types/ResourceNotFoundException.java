package com.atypon.decentraldbcluster.exceptions.types;

public class ResourceNotFoundException extends RuntimeException {
    public ResourceNotFoundException(String message) {
        super(message);
    }
}
