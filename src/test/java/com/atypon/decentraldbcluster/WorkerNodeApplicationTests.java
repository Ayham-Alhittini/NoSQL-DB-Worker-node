package com.atypon.decentraldbcluster;

import com.atypon.decentraldbcluster.query.databases.DatabaseQuery;
import com.atypon.decentraldbcluster.query.documents.DocumentQuery;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

public class WorkerNodeApplicationTests {

	@Test
	public void generalTest() throws JsonProcessingException {
		String query = """
				{
				  "originator" : "58c2dde5-246a-474f-b10d-bcc5d62d224b",
				  "database" : "test",
				  "collection" : null,
				  "databaseAction" : "CREATE"
				}
				""";

		ObjectMapper mapper = new ObjectMapper();

		DatabaseQuery databaseQuery = mapper.readValue(query, DatabaseQuery.class);

		System.out.println(databaseQuery);

		System.out.println(mapper.valueToTree(databaseQuery).toPrettyString());
	}


}

