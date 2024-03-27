package com.atypon.decentraldbcluster.services.documenting;

import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.error.ResourceNotFoundException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

@Service
public class DocumentService {

    private final ObjectMapper mapper;

    @Autowired
    public DocumentService(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public Document readDocument(String documentPath) throws IOException {

        Path filePath = Paths.get(documentPath);

        if (Files.isRegularFile(filePath)) {
            String jsonString = Files.readString(filePath);
            return mapper.readValue(jsonString, Document.class);
        }

        throw new ResourceNotFoundException("Document not exists");
    }

    public JsonNode readSchema(String collectionPath) throws IOException {
        String schemaPath = Paths.get(collectionPath, "schema.json").toString();

        Path filePath = Paths.get(schemaPath);
        if (Files.isRegularFile(filePath)) {
            String jsonString = Files.readString(filePath);
            return mapper.readTree(jsonString);
        }

        throw new ResourceNotFoundException("Schema not exists");
    }

    public List<Document> readDocumentsByDocumentsPathList(Set<String> documentsPath) throws IOException {
        List<Document> documents = new ArrayList<>();
        for (var document: documentsPath) {
            documents.add( readDocument(document) );
        }
        return documents;
    }

    public List<Document> readDocumentsByCollectionPath(String collectionPath) throws IOException {
        List<Document> documents = new ArrayList<>();

        try (Stream<Path> paths = Files.list(Paths.get(collectionPath, "documents"))) {
            paths.forEach(path -> {
                try {
                    var document = readDocument(path.toString());
                    documents.add(document);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        return documents;
    }

    public JsonNode patchDocument(JsonNode requestBody, JsonNode oldDocument) {
        ObjectNode newDocument = (ObjectNode) oldDocument;
        requestBody.fields().forEachRemaining(field -> newDocument.set(field.getKey(), field.getValue()));
        return newDocument;
    }
}
