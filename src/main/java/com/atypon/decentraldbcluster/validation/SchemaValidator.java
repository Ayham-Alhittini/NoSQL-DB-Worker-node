package com.atypon.decentraldbcluster.validation;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Service;

@Service
public class SchemaValidator {

    public void validateSchemaDataTypesIfExists(JsonNode schema) {
        if (schema == null) return;
        validateSchemaWithPath(schema, "schema");
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

}
