package com.atypon.decentraldbcluster.schema;

import com.atypon.decentraldbcluster.storage.disk.FileSystemService;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Paths;

@Service
public class SchemaCreator {
    private final FileSystemService fileSystemService;

    @Autowired
    public SchemaCreator(FileSystemService fileSystemService) {
        this.fileSystemService = fileSystemService;
    }

    //Facade design pattern
    public void validateAndCreateSchema(JsonNode schema, String collectionPath) throws IOException {
        validateSchemaWithPath(schema, "schema");
        createSchema(schema, collectionPath);
    }

    private void validateSchemaWithPath(JsonNode schema, String path) {
        if (!schema.isObject()) {
            validateDataType(schema, path);
            return;
        }

        schema.fields().forEachRemaining(field -> validateSchemaWithPath(field.getValue(), path + "." + field.getKey()));
    }

    private void validateDataType(JsonNode schema, String path) {
        String fieldValue = schema.asText().toUpperCase();
        try {
            AppDataType.valueOf(fieldValue);
            if (AppDataType.valueOf(fieldValue) == AppDataType.OBJECT) {
                throw new IllegalArgumentException(path + ", OBJECT type is not valid here.");
            }
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(path + ", invalid data type [" + fieldValue + "]");
        }
    }

    private void createSchema(JsonNode schema, String collectionPath) throws IOException {
        fileSystemService.saveFile(schema.toPrettyString(), Paths.get(collectionPath, "schema.json").toString() );
    }
}
