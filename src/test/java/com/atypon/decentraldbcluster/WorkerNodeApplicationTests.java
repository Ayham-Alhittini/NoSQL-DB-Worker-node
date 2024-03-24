package com.atypon.decentraldbcluster;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

public class WorkerNodeApplicationTests {

	@Test
	public void generalTest() throws Exception {
		String documentId = "\"dalkflk12-asdf\"";

		ObjectMapper objectMapper = new ObjectMapper();

		System.out.println(objectMapper.readTree(documentId));

	}

}
