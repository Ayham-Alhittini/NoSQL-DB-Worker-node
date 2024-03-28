package com.atypon.decentraldbcluster.services;

import com.atypon.decentraldbcluster.entity.Document;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Service
public class DocumentService {

    private final ObjectMapper mapper;
    private final FileSystemService fileSystemService;

    @Autowired
    public DocumentService(ObjectMapper mapper, FileSystemService fileSystemService) {
        this.mapper = mapper;
        this.fileSystemService = fileSystemService;
    }

    public Document readDocument(String documentPath) throws IOException {

        String fileContent = fileSystemService.loadFileContent(documentPath);
        return mapper.readValue(fileContent, Document.class);

    }

    public JsonNode readSchema(String collectionPath) throws IOException {
        String schemaPath = Paths.get(collectionPath, "schema.json").toString();

        String fileContent = fileSystemService.loadFileContent(schemaPath);
        return mapper.readTree(fileContent);
    }

    public List<Document> readDocumentsByDocumentsPathList(Set<String> documentsPath) throws IOException {
        List<Document> documents = new ArrayList<>();
        for (var documentPath: documentsPath) {
            documents.add( readDocument(documentPath) );
        }
        return documents;
    }

    public List<Document> readDocumentsByCollectionPath(String collectionPath) throws IOException {
        List<Document> documents = new ArrayList<>();
        List<String> filesPath = fileSystemService.getDirectoryFilesPath( Paths.get(collectionPath, "documents").toString() );
        for (String documentPath: filesPath) {
            documents.add( readDocument(documentPath) );
        }

        return documents;
    }

    public JsonNode patchDocument(JsonNode requestBody, JsonNode oldDocument) {
        ObjectNode newDocument = (ObjectNode) oldDocument;
        requestBody.fields().forEachRemaining(field -> newDocument.set(field.getKey(), field.getValue()));
        return newDocument;
    }
}
