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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

        String x = filePath.toString();

        if (Files.isRegularFile(filePath)) {
            String jsonString = Files.readString(filePath);
            return mapper.readTree(jsonString);
        }

        throw new ResourceNotFoundException("Document not exists");
    }

    public List<JsonNode> readAllDocumentsInCollection(String collectionPath, JsonNode filter) throws IOException {
        List<JsonNode> documents = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();

        try (Stream<Path> paths = Files.list(Paths.get(collectionPath, "documents"))) {
            paths.forEach(path -> {
                if (Files.isRegularFile(path)) {
                    try {
                        String jsonString = Files.readString(path);
                        JsonNode document = mapper.readTree(jsonString);

                        boolean validDocument = true;
                        if (filter != null) {

                            validDocument = isValidDocument(filter, document, validDocument);
                        }

                        if (validDocument)
                            documents.add(document);

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        return documents;
    }

    private boolean isValidDocument(JsonNode filter, JsonNode document, boolean validDocument) {
        var iterator = filter.fields();

        while (iterator.hasNext()) {
            var field = iterator.next();

            if (document.get(field.getKey()) == null) {
                validDocument = false;
                break;
            }

            if (!document.get(field.getKey()).equals(field.getValue())) {
                validDocument = false;
                break;
            }
        }
        return validDocument;
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
