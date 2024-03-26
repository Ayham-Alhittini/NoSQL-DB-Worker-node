package com.atypon.decentraldbcluster.validation;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Service;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;

@Service
public class DocumentValidator {
    private static final List<String> DATE_FORMATS = Arrays.asList(
            "yyyy-MM-dd'T'HH:mm:ss'Z'",
            "yyyy-MM-dd'T'HH:mm:ss",
            "yyyy-MM-dd HH:mm:ss"
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
