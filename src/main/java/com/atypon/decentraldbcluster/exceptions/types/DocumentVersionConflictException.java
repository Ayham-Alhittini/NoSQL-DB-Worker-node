package com.atypon.decentraldbcluster.exceptions.types;

public class DocumentVersionConflictException extends RuntimeException{
    public DocumentVersionConflictException() {
        super("Document version conflict");
    }
}
