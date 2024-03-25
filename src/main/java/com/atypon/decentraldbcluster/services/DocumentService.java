package com.atypon.decentraldbcluster.services;

import com.atypon.decentraldbcluster.index.ObjectId;
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

    public String constructDocumentPath(String collectionPath, String documentId) {
        return Paths.get( collectionPath , "documents", documentId + ".json").toString();
    }


    public JsonNode readDocument(String documentPath) throws IOException {

        Path filePath = Paths.get(documentPath);

        if (Files.isRegularFile(filePath)) {
            String jsonString = Files.readString(filePath);
            return mapper.readTree(jsonString);
        }

        throw new ResourceNotFoundException("Document not exists");
    }

    public JsonNode readSchema(String collectionPath) throws IOException {
        String schemaPath = Paths.get(collectionPath, "schema.json").toString();
        return readDocument(schemaPath);
    }

    public List<JsonNode> readDocumentsByDocumentsPathList(Set<String> documentsPath) throws IOException {
        List<JsonNode> documents = new ArrayList<>();
        for (var document: documentsPath) {
            documents.add( readDocument(document) );
        }
        return documents;
    }

    public List<JsonNode> readDocumentsByCollectionPath(String collectionPath) throws IOException {
        List<JsonNode> documents = new ArrayList<>();

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


    public ObjectId createAndAppendDocumentId(Map<String, Object> document) {
        ObjectId objectId = new ObjectId();
        document.remove("_id");//_id is system generated field

        Map<String, Object> orderedData = new LinkedHashMap<>();
        orderedData.put("_id", objectId.toHexString());
        orderedData.putAll(document);

        document.clear();
        document.putAll(orderedData);
        return objectId;
    }

    public JsonNode updateDocument(JsonNode requestBody, JsonNode oldDocument) {
        ObjectNode newDocument = (ObjectNode) oldDocument;
        requestBody.fields().forEachRemaining(field -> {
            if (!field.getKey().equals("_id"))
                newDocument.put(field.getKey(), field.getValue());
        });
        return newDocument;
    }


}
