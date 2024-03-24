package com.atypon.decentraldbcluster.services;

import com.atypon.decentraldbcluster.entity.AppDataType;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Service;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;

@Service
public class JsonSchemaValidator {

    private static final List<String> DATE_FORMATS = Arrays.asList(
            "yyyy-MM-dd'T'HH:mm:ss'Z'",
            "yyyy-MM-dd'T'HH:mm:ss",
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd"
    );

    public void doesDocumentMatchSchema(JsonNode data, JsonNode schema, boolean fieldsRequired) {
        doesDocumentMatchSchemaWithPath(data, schema, "schema", fieldsRequired);
    }

    private void doesDocumentMatchSchemaWithPath(JsonNode document, JsonNode schema, String path, boolean fieldsRequired) {
        AppDataType documentType = getDataType(document);
        String schemaType = schema.isObject() ? "OBJECT" : schema.asText();
        AppDataType schemaDataType = AppDataType.valueOf(schemaType.toUpperCase());

        if (!isMatch(documentType, schemaDataType)) {
            throw new IllegalArgumentException("Type mismatch at " + path + ": expected " + schemaDataType + ", found " + documentType);
        }

        if (documentType == AppDataType.OBJECT) {
            schema.fields().forEachRemaining(field -> {
                if (field.getKey().equals("_id")) return;

                JsonNode documentField = document.get(field.getKey());
                if (documentField == null) {
                    if (fieldsRequired) {
                        throw new IllegalArgumentException("Missing field at " + path + "." + field.getKey());
                    }
                    return;
                }
                doesDocumentMatchSchemaWithPath(documentField, schema.get(field.getKey()), path + "." + field.getKey(), fieldsRequired);
            });
        }
    }

    public void validateSchema(JsonNode schema) {
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

    //--------------------------------------------------------------------

    private boolean isMatch(AppDataType dataType, AppDataType schemaDataType) {
        return dataType == schemaDataType || (dataType == AppDataType.INTEGER && schemaDataType == AppDataType.DECIMAL);
    }

    private AppDataType getDataType(JsonNode node) {
        if (node.isObject()) return AppDataType.OBJECT;
        if (node.isTextual()) return isDateStringValid(node.asText()) ? AppDataType.DATETIME : AppDataType.STRING;
        if (node.isIntegralNumber()) return AppDataType.INTEGER;
        if (node.isDouble()) return AppDataType.DECIMAL;
        if (node.isBoolean()) return AppDataType.BOOLEAN;
        if (node.isArray()) return AppDataType.ARRAY;
        throw new IllegalArgumentException("Unsupported data type, " + node);
    }

    private boolean isDateStringValid(String dateString) {
        return DATE_FORMATS.stream().anyMatch(format -> {
            SimpleDateFormat sdf = new SimpleDateFormat(format);
            sdf.setLenient(false);
            try {
                sdf.parse(dateString);
                return true;
            } catch (ParseException e) {
                return false;
            }
        });
    }
}
