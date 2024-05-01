package com.atypon.decentraldbcluster.schema;

import com.atypon.decentraldbcluster.exceptions.types.ResourceNotFoundException;
import com.atypon.decentraldbcluster.storage.disk.FileSystemService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;

@Service
public class SchemaValidator {
    private final ObjectMapper mapper;
    private final FileSystemService fileSystemService;

    @Autowired
    public SchemaValidator(ObjectMapper mapper, FileSystemService fileSystemService) {
        this.mapper = mapper;
        this.fileSystemService = fileSystemService;
    }

    public void validateDocument(JsonNode data, String schemaCollection, boolean fieldsRequired) throws IOException {
        JsonNode schema = loadSchema(schemaCollection);
        if (schema == null) return;
        validateDocumentAgainstSchema(data, schema, "schema", fieldsRequired);
    }

    private JsonNode loadSchema(String collectionPath) throws IOException {
        String schemaPath = constructSchemaPath(collectionPath);
        return readSchemaFromPath(schemaPath);
    }

    private String constructSchemaPath(String collectionPath) {
        return Paths.get(collectionPath, "schema.json").toString();
    }

    private JsonNode readSchemaFromPath(String schemaPath) throws IOException {
        try {
            String fileContent = fileSystemService.loadFileContent(schemaPath);
            return mapper.readTree(fileContent);
        } catch (ResourceNotFoundException e) {
            return null;
        }
    }

    private void validateDocumentAgainstSchema(JsonNode document, JsonNode schema, String path, boolean fieldsRequired) {
        schema.fields().forEachRemaining(field -> validateField(document, field, path, fieldsRequired));
    }

    private void validateField(JsonNode document, Map.Entry<String, JsonNode> field, String path, boolean fieldsRequired) {
        String fieldName = field.getKey();
        JsonNode documentField = document.get(fieldName);
        JsonNode schemaField = field.getValue();
        String fieldPath = path + "." + fieldName;

        if (documentField == null && fieldsRequired) {
            throw new IllegalArgumentException("Missing field at " + fieldPath);
        }

        if (documentField != null) {
            validateDataType(documentField, schemaField, fieldPath, fieldsRequired);
        }
    }

    private void validateDataType(JsonNode documentField, JsonNode schemaField, String path, boolean fieldsRequired) {
        AppDataType documentType = getDataType(documentField);
        AppDataType schemaDataType = getSchemaDataType(schemaField);

        if (!isMatch(documentType, schemaDataType)) {
            throw new IllegalArgumentException("Type mismatch at " + path + ": expected " + schemaDataType + ", found " + documentType);
        }

        if (documentType == AppDataType.OBJECT) {
            validateDocumentAgainstSchema(documentField, schemaField, path, fieldsRequired);
        }
    }

    private AppDataType getSchemaDataType(JsonNode schemaField) {
        String schemaType = schemaField.isObject() ? "OBJECT" : schemaField.asText();
        return AppDataType.valueOf(schemaType.toUpperCase());
    }

    private boolean isMatch(AppDataType dataType, AppDataType schemaDataType) {
        return dataType == schemaDataType || (dataType == AppDataType.INTEGER && schemaDataType == AppDataType.DECIMAL);
    }

    private AppDataType getDataType(JsonNode node) {
        if (node.isObject()) return AppDataType.OBJECT;
        if (node.isTextual()) return AppDataType.STRING;
        if (node.isIntegralNumber()) return AppDataType.INTEGER;
        if (node.isDouble()) return AppDataType.DECIMAL;
        if (node.isBoolean()) return AppDataType.BOOLEAN;
        if (node.isArray()) return AppDataType.ARRAY;
        throw new IllegalArgumentException("Unsupported data type, " + node);
    }


}
