package com.atypon.decentraldbcluster;


import org.junit.jupiter.api.Test;

public class WorkerNodeApplicationTests {
	@Test
	public void generalTest() {
		System.out.println(extractDocumentIdFromDocumentPath("storage/test/users/documents/documentId.json"));
	}
	public static String extractDocumentIdFromDocumentPath(String documentPath) {
		String temp = documentPath.substring(documentPath.indexOf("documents/") + "documents/".length());
		return temp.substring(0, temp.indexOf(".json"));
	}
}
