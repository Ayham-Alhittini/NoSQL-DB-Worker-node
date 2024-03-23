package com.atypon.decentraldbcluster.services;

import com.atypon.decentraldbcluster.entity.ObjectId;
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
import java.util.LinkedHashMap;
import java.util.Map;

@Service
public class DocumentService {

    private final ObjectMapper mapper;

    @Autowired
    public DocumentService(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public String constructDocumentPath(String userDirectory, String database, String collection, String documentId) {
        return Paths.get(userDirectory, database, collection, "documents", documentId + ".json").toString();
    }


    public JsonNode readDocument(String documentPath) throws IOException {

        Path filePath = Paths.get(FileStorageService.appendToBaseDirectory(documentPath));

        if (Files.isRegularFile(filePath)) {
            String jsonString = Files.readString(filePath);
            return mapper.readTree(jsonString);
        }

        throw new ResourceNotFoundException("Document not exists");
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
