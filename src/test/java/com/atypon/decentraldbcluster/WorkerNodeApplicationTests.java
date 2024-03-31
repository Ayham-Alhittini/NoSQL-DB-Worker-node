package com.atypon.decentraldbcluster;


import com.atypon.decentraldbcluster.config.NodeConfiguration;
import org.junit.jupiter.api.Test;

public class WorkerNodeApplicationTests {

	@Test
	public void generalTest() {
		String url = "http://localhost:8081/api/document/test/users/updateDocument/f97981be-88d4-49da-bb85-00f0f3b9cc40?expectedVersion=1";

		url = url.replace(NodeConfiguration.getCurrentNodeAddress(), NodeConfiguration.getNodeAddress(8082));

		System.out.println(url);

	}
}

