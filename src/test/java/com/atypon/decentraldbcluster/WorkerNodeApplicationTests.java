package com.atypon.decentraldbcluster;

import com.atypon.decentraldbcluster.entity.Document;
import com.atypon.decentraldbcluster.query.base.Query;
import com.atypon.decentraldbcluster.query.documents.DocumentQueryBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

public class WorkerNodeApplicationTests {

	@Test
	public void generalTest() throws JsonProcessingException {

		ObjectMapper mapper = new ObjectMapper();

		String newContent = """
    			{
    				"something": "231"
    			}
				""";

		String documentContent = """
				{
				    "id": "0cbd5046-5983-4cf4-9bbb-ead6bb202662",
				    "content": {
				        "something": "231"
				    },
				    "version": 3,
				    "affinityPort": 8081
				}
				""";


		Query query = new DocumentQueryBuilder()
				.withOriginator("Ayham")
				.withDatabase("test")
				.withCollection("users")
				.updateDocument( mapper.readValue(documentContent, Document.class), mapper.readTree(newContent))
				.build();

		System.out.println( mapper.valueToTree(query).toPrettyString() );

	}


}

