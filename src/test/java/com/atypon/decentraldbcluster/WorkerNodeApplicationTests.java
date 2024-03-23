package com.atypon.decentraldbcluster;


import com.atypon.decentraldbcluster.entity.AppDataType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class WorkerNodeApplicationTests {

	@Test
	public void contextLoads() throws JsonProcessingException {
		String jsonString = """
				{
				  "_id" : "STRING",
				  "age": "INTEGER",
				  "tail": "DECIMAL",
				  "siblings": "ARRAY",
				  "isAdult": "BOOLEAN",
				  "name" : "STRING",
				  "gender" : "STRING",
				  "email" : "STRING",
				  "bornDate": "DATETIME",
				  "address": {
				       "street": "STRING",
				       "city": "STRING",
				       "state": "STRING",
				       "zipCode": "STRING",
				       "test": {"testFiled": "BOOLEAN"}
				    }
				}
				""";

		ObjectMapper mapper = new ObjectMapper();

		validateSchema(mapper.readTree(jsonString));

	}

	public void validateSchema(JsonNode schema) {
		if (schema.isObject()) {

			var fields = schema.fields();

			while (fields.hasNext()) {
				var filed = fields.next();
				validateSchema(filed.getValue());
			}
			return;
		}

		String filedValue = schema.asText().toUpperCase();

		try {
			AppDataType fieldType = AppDataType.valueOf(filedValue);
			if (fieldType == AppDataType.OBJECT) throw new Exception();
		} catch (Exception e) {
			throw new IllegalArgumentException(filedValue + " is not a valid data type");
		}
	}


	@Test
	public void generalTest() {
		Map<String, String> map = new LinkedHashMap<>();
//		map.put("_id", "adfdaf");
		map.put("gender", "male");

		map.remove("_id");

		for (var i: map.entrySet()) System.out.println(i);


	}


	@Test
	public void testDatetime() {
		System.out.println(isDateStringValid("2023-03 12:12:12Z"));
	}

	private boolean isDateStringValid(String dateString) {
		List<String> dateFormats = Arrays.asList(
				"yyyy-MM-dd'T'HH:mm:ss'Z'",
				"yyyy-MM-dd'T'HH:mm:ss",
				"yyyy-MM-dd HH:mm:ss",
				"yyyy-MM-dd"
		);

		for (String format : dateFormats) {
			SimpleDateFormat sdf = new SimpleDateFormat(format);
			sdf.setLenient(true);
			try {
				sdf.parse(dateString);
				return true;
			} catch (ParseException ignored) {}
		}

		return false;
	}

}
