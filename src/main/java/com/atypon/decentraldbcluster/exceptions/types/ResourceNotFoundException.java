package com.atypon.decentraldbcluster.exceptions.types;

public class ResourceNotFoundException extends RuntimeException {
    //Todo: consider handle it sometimes no need to throw it always
    public ResourceNotFoundException(String message) {
        super(message);
    }
}
